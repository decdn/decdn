use alloy::primitives::B256;
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;

use super::*;

const OPERATOR: Address = Address::repeat_byte(0x0E);

/// A block at `timestamp`, as `eth_getBlockByNumber` returns it.
fn block(timestamp: u64) -> alloy::rpc::types::Block {
    let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
    block.header.inner.timestamp = timestamp;
    block
}

#[tokio::test]
async fn block_at_or_after_binary_searches_the_headers() {
    // Blocks 0..=7 at 100, 110, …, 170; target 135 is block 4 (140). The
    // search reads `hi` first, then probes 3, 5, 4.
    let asserter = Asserter::new();
    for probe in [7u64, 3, 5, 4] {
        asserter.push_success(&block(100 + 10 * probe));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 135, 0, 7).await.unwrap(), 4);
}

/// A block whose time is exactly the target is the period's first block: the
/// cutoff is inclusive, as the daemon's `landed_at_unix >= from` is.
#[tokio::test]
async fn block_at_or_after_takes_the_block_at_the_target_second() {
    let asserter = Asserter::new();
    for probe in [7u64, 3, 5, 4] {
        asserter.push_success(&block(100 + 10 * probe));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 140, 0, 7).await.unwrap(), 4);
}

#[tokio::test]
async fn block_at_or_after_past_head_is_an_empty_period() {
    let asserter = Asserter::new();
    asserter.push_success(&block(170));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 500, 0, 7).await.unwrap(), 8);
    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 500, 9, 7).await.unwrap(), 8);
}

fn select(pool: Option<&str>, rpc: Option<&str>) -> cli::PoolSelectArgs {
    cli::PoolSelectArgs {
        pool: pool.map(str::to_owned),
        pool_rpc_url: rpc.map(str::to_owned),
    }
}

const TWO_CHAINS: &str = r#"
[blockchain]
rpc_url = "http://governance.example"
chain_id = 11155111
get_logs_max_block_span = 100

[[blockchain.payment_chains]]
rpc_url = "http://base.example"
chain_id = 84532
payment_pools = ["0x0000000000000000000000000000000000000042"]

[[blockchain.payment_chains]]
rpc_url = "http://arb.example"
chain_id = 421614
payment_pools = ["0x0000000000000000000000000000000000000043"]
"#;

/// Every configured pool is scanned on its own payment chain, never the
/// governance chain; `--pool` narrows it to one.
#[test]
fn every_configured_pool_is_a_scan_target() {
    let file: chain_ctx::FileConfig = toml::from_str(TWO_CHAINS).expect("parses");
    let targets = scan_targets(&select(None, None), Some(9), &file).expect("resolves");
    let got: Vec<_> = targets
        .iter()
        .map(|t| {
            (
                t.pool.deployment.chain_id,
                t.pool.rpc_url.as_str(),
                t.from_block,
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (84_532, "http://base.example", 9),
            (421_614, "http://arb.example", 9)
        ]
    );
    assert_eq!(scan_span(&file).unwrap().get(), 100);

    let one = scan_targets(
        &select(
            Some("eip155:421614:0x0000000000000000000000000000000000000043"),
            None,
        ),
        Some(1),
        &file,
    )
    .expect("resolves");
    assert_eq!(one.len(), 1);
    assert_eq!(
        one.first().map(|t| t.pool.rpc_url.as_str()),
        Some("http://arb.example")
    );
}

/// With no known target, the first block must be spelled out.
#[test]
fn an_unknown_chain_needs_from_block() {
    let file: chain_ctx::FileConfig = toml::from_str(TWO_CHAINS).expect("parses");
    let err = scan_targets(&select(None, None), None, &file).expect_err("no from block");
    assert!(err.to_string().contains("--from-block"), "{err}");
    let empty: chain_ctx::FileConfig =
        toml::from_str("[blockchain]\nchain_id = 31337\n").expect("parses");
    let err = scan_targets(&select(None, None), Some(1), &empty).expect_err("no pool");
    assert!(err.to_string().contains("--pool"), "{err}");
}

fn row(pool_byte: u8, block: u64, net: u64, gross: u64, bytes: u64) -> SettlementRecord {
    SettlementRecord {
        deployment: decdn_incentive::Deployment {
            chain_id: 1,
            payment_pool: Address::repeat_byte(pool_byte),
        },
        block_number: block,
        log_index: 0,
        tx_hash: B256::repeat_byte(u8::try_from(block).unwrap_or(0)),
        landed_at_unix: 0,
        gross_micro_usdc: gross,
        net_micro_usdc: net,
        bytes_delivered: bytes,
        operator: OPERATOR,
    }
}

/// Each pool reports its own net, gross and bytes and its pending protocol
/// share; the period totals span every pool; the last settlement is the
/// latest by block time across chains.
#[test]
fn aggregate_splits_per_pool_and_sums_the_totals() {
    let mut a_last = row(0xA, 3, 60, 100, 4_096);
    a_last.landed_at_unix = 1_000;
    let mut b_last = row(0xB, 9, 6, 10, 512);
    b_last.landed_at_unix = 2_000;
    let share = ProtocolShare {
        pending_micro_usdc: 44,
        pending_bytes: 1_800,
        min_sweep_micro_usdc: 1_000_000,
    };
    let scans = vec![
        PoolScan {
            pool: "eip155:1:0x0a".to_owned(),
            from_unix: 500,
            rows: vec![row(0xA, 1, 30, 50, 1_024), a_last],
            in_day: 1,
            in_week: 2,
            in_since: 0,
            share: Some(share.clone()),
        },
        PoolScan {
            pool: "eip155:2:0x0b".to_owned(),
            from_unix: 800,
            rows: vec![b_last],
            in_day: 1,
            in_week: 1,
            in_since: 1,
            share: None,
        },
    ];
    let cutoffs = Cutoffs {
        day: 9_000,
        week: 3_000,
        since: Some(1_500),
    };
    let resp = aggregate(OPERATOR, &scans, 10_000, cutoffs);

    let pools: Vec<_> = resp
        .pools
        .iter()
        .map(|p| {
            (
                p.pool.as_str(),
                p.settled.net_micro_usdc,
                p.settled.gross_micro_usdc,
                p.settled.bytes_settled,
                p.settled.settlements,
                p.protocol_share.clone(),
            )
        })
        .collect();
    assert_eq!(
        pools,
        vec![
            ("eip155:1:0x0a", 90, 150, 5_120, 2, Some(share)),
            ("eip155:2:0x0b", 6, 10, 512, 1, None),
        ]
    );
    assert_eq!(resp.covered_from_unix, 500);
    assert_eq!(
        (
            resp.since_ledger_start.net_micro_usdc,
            resp.since_ledger_start.settlements
        ),
        (96, 3)
    );
    assert_eq!(
        (resp.last_24h.from_unix, resp.last_24h.net_micro_usdc),
        (9_000, 66)
    );
    assert_eq!(resp.last_7d.net_micro_usdc, 96);
    let since = resp.since.expect("asked for");
    assert_eq!((since.from_unix, since.net_micro_usdc), (1_500, 6));
    let last = resp.last_settlement.expect("a last settlement");
    assert_eq!((last.block_number, last.at_unix), (9, 2_000));
    assert!(resp.pending.is_none(), "the chain sees no lane state");
}
