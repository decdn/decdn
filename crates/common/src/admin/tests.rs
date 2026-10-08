use super::*;

/// `wait_admin` is required: `{}` is refused rather than silently read as
/// the SIGTERM-equivalent drain.
#[test]
fn drain_request_requires_wait_admin() {
    assert!(
        serde_json::from_str::<DrainRequest>("{}").is_err(),
        "a DrainRequest without wait_admin must not deserialize"
    );
    let req: DrainRequest = serde_json::from_str(r#"{"wait_admin":true}"#)
        .expect("an explicit wait_admin must deserialize");
    assert!(req.wait_admin);
}

/// `dry_run` is required: `{"hash": …}` alone is refused rather than read
/// as a real evict.
#[test]
fn evict_request_requires_dry_run() {
    assert!(
        serde_json::from_str::<EvictRequest>(r#"{"hash":"00"}"#).is_err(),
        "an EvictRequest without dry_run must not deserialize"
    );
}

/// `StatusResponse` round-trips through serde unchanged — guards the
/// nested DTO shapes (`RoutingHealth` / `BucketStat` /
/// `RecordStoreHealth` / `RepublishHealth`) the CLI and server both
/// (de)serialize.
#[test]
fn status_response_round_trips() {
    let resp = StatusResponse {
        node_id: "abc".to_string(),
        routing: RoutingHealth {
            total_peers: 21,
            non_empty_buckets: 2,
            buckets: vec![
                BucketStat { index: 0, fill: 1 },
                BucketStat {
                    index: 255,
                    fill: 20,
                },
            ],
            bucket_capacity: 20,
            refresh_interval_s: 3600,
            last_refresh_us: Some(1_700_000_000_000_000),
        },
        known_stakers: 7,
        record_store: RecordStoreHealth {
            records: 12,
            capacity: 100_000,
        },
        republish: RepublishHealth {
            scheduled_records: 5,
        },
        chain_denied_origins: 3,
        operator_address: Some("0x52908400098527886E0F7030069857D2E4169EE7".to_string()),
    };
    let json = serde_json::to_string(&resp).expect("serialize StatusResponse");
    let back: StatusResponse = serde_json::from_str(&json).expect("deserialize StatusResponse");
    assert_eq!(back.node_id, "abc");
    assert_eq!(back.routing.total_peers, 21);
    assert_eq!(back.routing.buckets.len(), 2);
    assert_eq!(back.routing.bucket_capacity, 20);
    assert_eq!(back.routing.last_refresh_us, Some(1_700_000_000_000_000));
    assert_eq!(back.known_stakers, 7);
    assert_eq!(back.record_store.capacity, 100_000);
    assert_eq!(back.republish.scheduled_records, 5);
    assert_eq!(back.chain_denied_origins, 3);
    assert_eq!(
        back.operator_address.as_deref(),
        Some("0x52908400098527886E0F7030069857D2E4169EE7")
    );
}

/// `BuyerPoolsResponse` round-trips through serde unchanged — guards the
/// nested shape both the server and `decdn node pools` (de)serialize, and
/// pins that `skipped` survives independently of `pools` (an empty `pools`
/// beside a non-empty `skipped` is a real state, #2078).
#[test]
fn buyer_pools_response_round_trips() {
    let resp = BuyerPoolsResponse {
        pools: vec![BuyerPoolSnapshot {
            pool_id: "0xabcd".to_string(),
            chain_id: 421_614,
            payment_pool: "0x00dd".to_string(),
            owner: "0x52908400098527886E0F7030069857D2E4169EE7".to_string(),
            token: "0x00cc".to_string(),
            deposit_micro_usdc: 10_000_000,
            lanes: vec![BuyerLaneSnapshot {
                voucher_signer: "0x00aa".to_string(),
                provider: "0x00bb".to_string(),
                last_amount_micro_usdc: 191_205,
                last_bytes_delivered: 4_194_304,
            }],
        }],
        skipped: vec!["0x4444".to_string()],
    };
    let json = serde_json::to_string(&resp).expect("serialize BuyerPoolsResponse");
    let back: BuyerPoolsResponse =
        serde_json::from_str(&json).expect("deserialize BuyerPoolsResponse");
    let pool = back.pools.first().expect("one pool");
    assert_eq!(pool.pool_id, "0xabcd");
    assert_eq!(pool.chain_id, 421_614);
    assert_eq!(pool.deposit_micro_usdc, 10_000_000);
    let lane = pool.lanes.first().expect("one lane");
    assert_eq!(lane.provider, "0x00bb");
    assert_eq!(lane.last_amount_micro_usdc, 191_205);
    assert_eq!(lane.last_bytes_delivered, 4_194_304);
    assert_eq!(back.skipped, vec!["0x4444".to_string()]);
}

/// `LanesResponse` round-trips through serde unchanged — guards the
/// nested `LaneSnapshot` shape both the server and `decdn node
/// lanes` (de)serialize (issue #749).
#[test]
fn lanes_response_round_trips() {
    let resp = LanesResponse {
        redeem_threshold_micro_usdc: 1_000_000,
        lanes: vec![
            LaneSnapshot {
                pool_id: "0xabcd".to_string(),
                counterparty: "0x00aa".to_string(),
                voucher_signer: "0x00cc".to_string(),
                last_nonce: 7,
                outstanding_micro_usdc: 2_500_000,
                deposit_micro_usdc: 10_000_000,
                seconds_since_last_voucher: Some(42),
                settlement_eligible: true,
            },
            LaneSnapshot {
                pool_id: "0xbeef".to_string(),
                counterparty: "0x00bb".to_string(),
                voucher_signer: "0x00bb".to_string(),
                last_nonce: 0,
                outstanding_micro_usdc: 0,
                deposit_micro_usdc: 5_000_000,
                seconds_since_last_voucher: None,
                settlement_eligible: false,
            },
        ],
    };
    let json = serde_json::to_string(&resp).expect("serialize LanesResponse");
    let back: LanesResponse = serde_json::from_str(&json).expect("deserialize LanesResponse");
    assert_eq!(back.redeem_threshold_micro_usdc, 1_000_000);
    assert_eq!(back.lanes.len(), 2);
    let first = back.lanes.first().expect("first lane");
    assert_eq!(first.pool_id, "0xabcd");
    assert_eq!(first.counterparty, "0x00aa");
    assert_eq!(
        first.voucher_signer, "0x00cc",
        "a delegated signer must survive the round trip distinct from the funder"
    );
    assert_eq!(first.last_nonce, 7);
    assert_eq!(first.outstanding_micro_usdc, 2_500_000);
    assert_eq!(first.deposit_micro_usdc, 10_000_000);
    assert_eq!(first.seconds_since_last_voucher, Some(42));
    assert!(first.settlement_eligible);
    let second = back.lanes.get(1).expect("second lane");
    assert_eq!(second.seconds_since_last_voucher, None);
    assert!(!second.settlement_eligible);
}
