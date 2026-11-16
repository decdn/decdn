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

fn args(
    rpc_url: Option<&str>,
    payment_pool: Option<&str>,
    from_block: Option<u64>,
) -> cli::EarningsArgs {
    cli::EarningsArgs {
        admin_url: None,
        config: None,
        json: false,
        timeout_ms: 5_000,
        since: None,
        address: Some(OPERATOR.to_string()),
        rpc_url: rpc_url.map(str::to_owned),
        payment_pool_address: payment_pool.map(str::to_owned),
        from_block,
    }
}

#[test]
fn flags_override_everything() {
    let pool = Address::repeat_byte(0x42);
    let coords = resolve_coordinates(
        &args(
            Some("http://rpc.example"),
            Some(&pool.to_string()),
            Some(77),
        ),
        &chain_ctx::FileConfig::default(),
    )
    .expect("flags resolve");
    assert_eq!(coords.rpc_url, "http://rpc.example");
    assert_eq!(coords.payment_pool, pool);
    assert_eq!(coords.from_block, 77);
}

/// The coordinates are the payment chain's: its rpc, chain id and first pool,
/// never the governance chain's.
#[test]
fn the_config_files_first_payment_chain_fills_what_flags_leave_unset() {
    let file: chain_ctx::FileConfig = toml::from_str(
        r#"
[blockchain]
rpc_url = "http://governance.example"
chain_id = 11155111
get_logs_max_block_span = 100

[[blockchain.payment_chains]]
rpc_url = "http://payment.example"
chain_id = 84532
payment_pools = ["0x0000000000000000000000000000000000000042"]
"#,
    )
    .expect("parses");
    let coords = resolve_coordinates(&args(None, None, Some(9)), &file).expect("resolves");
    assert_eq!(coords.rpc_url, "http://payment.example");
    assert_eq!(coords.chain_id, 84_532);
    assert_eq!(coords.payment_pool, Address::with_last_byte(0x42));
    assert_eq!(coords.span.get(), 100);
    let coords = resolve_coordinates(&args(Some("http://flag.example"), None, Some(5)), &file)
        .expect("resolves");
    assert_eq!(coords.rpc_url, "http://flag.example");
    assert_eq!(coords.from_block, 5);
}

#[test]
fn an_unknown_chain_needs_every_coordinate_spelled_out() {
    let file: chain_ctx::FileConfig =
        toml::from_str("[blockchain]\nchain_id = 31337\n").expect("parses");
    let err = resolve_coordinates(&args(None, None, None), &file).expect_err("no rpc");
    assert!(err.to_string().contains("--rpc-url"), "{err}");
    let err = resolve_coordinates(&args(Some("http://x"), None, None), &file).expect_err("no pool");
    assert!(err.to_string().contains("--payment-pool-address"), "{err}");
    let pool = Address::repeat_byte(0x42).to_string();
    let err = resolve_coordinates(&args(Some("http://x"), Some(&pool), None), &file)
        .expect_err("no from block");
    assert!(err.to_string().contains("--from-block"), "{err}");
    resolve_coordinates(&args(Some("http://x"), Some(&pool), Some(1)), &file)
        .expect("all spelled out");
}
