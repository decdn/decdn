//! The `/_status` contract with the dashboard: a fully populated status of
//! each mode, pinned to the JSON fixtures the dashboard's tests read
//! (`crates/cli/dashboard/src/fixtures/`). A change to the body fails here
//! until the fixtures are rewritten with `UPDATE_STATUS_FIXTURES=1`, and the
//! dashboard's `api.ts` must then match them.

use std::path::{Path, PathBuf};

use alloy::primitives::B256;
use decdn_common::admin::{BuyerLaneSnapshot, BuyerPoolSnapshot, BuyerPoolsResponse};

use super::super::allowance::BucketView;
use super::super::metrics::Counters;
use super::super::pointer::Held;
use super::super::worker::{DelegateLanes, PoolSnapshot};
use super::super::{ActivePullView, Capability, PayerLabel};
use super::*;

const OWNER: Address = Address::repeat_byte(0xab);
const DELEGATE: Address = Address::repeat_byte(0xcd);
const NODE: Address = Address::repeat_byte(0xb2);

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("dashboard/src/fixtures")
        .join(name)
}

/// `status` serialized equals the fixture `name`, or rewrites it when
/// `UPDATE_STATUS_FIXTURES` is set.
fn pin(name: &str, status: &Status) {
    let json = serde_json::to_string_pretty(status).unwrap() + "\n";
    let path = fixture(name);
    if std::env::var_os("UPDATE_STATUS_FIXTURES").is_some() {
        std::fs::write(&path, &json).unwrap();
        return;
    }
    let pinned = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        pinned == json,
        "{} does not match /_status: rerun with UPDATE_STATUS_FIXTURES=1, then make \
         crates/cli/dashboard/src/api.ts match it\n{json}",
        path.display()
    );
}

fn counters() -> Counters {
    Counters {
        bytes_served: 48_318_382_080,
        requests_ok: 128_400,
        requests_not_found: 312,
        requests_refused: 41,
        requests_allowance_exhausted: 7,
        pull_failures_funding: 1,
        pull_failures_other: 3,
        pulls_active: 2,
        pointer_updates: 5,
    }
}

fn lane(signer: Address, amount: u64, bytes: u64) -> BuyerLaneSnapshot {
    BuyerLaneSnapshot {
        voucher_signer: signer.to_string(),
        provider: NODE.to_string(),
        last_amount_micro_usdc: amount,
        last_bytes_delivered: bytes,
    }
}

fn base(mode: Mode) -> Status {
    Status {
        mode,
        version: "0.0.0-fixture",
        started_unix: 1_793_000_000,
        uptime_secs: 93_784,
        listen: "127.0.0.1:8484".parse().unwrap(),
        chain_id: 421_614,
        payment_pool: Address::repeat_byte(0x35),
        counters: counters(),
        active_pulls: Vec::new(),
        capabilities: Vec::new(),
        allowance: None,
        pointers: None,
        pools: None,
        pool_error: None,
    }
}

#[test]
fn a_public_status_keeps_its_shape() {
    let pool = B256::repeat_byte(0x01);
    let status = Status {
        active_pulls: vec![ActivePullView {
            hash: alloy::hex::encode([0xea; 32]),
            namespace: Some(7),
            range: Some((1_048_576, 4_194_304)),
            payer: PayerLabel::Pool,
            since_secs: 12,
            delivered_bytes: 3_400_000,
        }],
        allowance: Some(vec![
            BucketView {
                namespace: None,
                per_day: Some(21_474_836_480),
                used: 17_179_869_184,
                remaining: Some(4_294_967_296),
                resets_in_secs: 30_000,
            },
            BucketView {
                namespace: Some(9),
                per_day: None,
                used: 0,
                remaining: None,
                resets_in_secs: 30_000,
            },
        ]),
        pointers: Some(vec![Held {
            namespace: 7,
            root: B256::repeat_byte(0xcd),
            seq: 12,
            signer: OWNER,
        }]),
        pools: Some(PoolSnapshot {
            pools: BuyerPoolsResponse {
                pools: vec![BuyerPoolSnapshot {
                    pool_id: format!("{pool:#x}"),
                    chain_id: 421_614,
                    payment_pool: Address::repeat_byte(0x35).to_string(),
                    owner: OWNER.to_string(),
                    token: Address::repeat_byte(0xcc).to_string(),
                    deposit_micro_usdc: 10_000_000,
                    spent_micro_usdc: 3_250_000,
                    lanes: vec![lane(OWNER, 3_250_000, 48_000_000_000)],
                }],
                skipped: vec![format!("{:#x}", B256::repeat_byte(0x44))],
            },
            delegate_lanes: Vec::new(),
        }),
        ..base(Mode::Public)
    };
    pin("status-public.json", &status);
}

#[test]
fn a_loopback_status_keeps_its_shape() {
    let pool = B256::repeat_byte(0x02);
    let status = Status {
        active_pulls: vec![ActivePullView {
            hash: alloy::hex::encode([0xbb; 32]),
            namespace: None,
            range: None,
            payer: PayerLabel::Capability { signer: DELEGATE },
            since_secs: 2,
            delivered_bytes: 65_536,
        }],
        capabilities: vec![Capability {
            cap: 4_000_000,
            expiry: 1_900_000_000,
            origin: Some("https://decdn.org".to_owned()),
            pool: pool.to_string(),
            scope: "decdn.org".to_owned(),
            signer: DELEGATE.to_string(),
            spent: false,
        }],
        pools: Some(PoolSnapshot {
            pools: BuyerPoolsResponse::default(),
            delegate_lanes: vec![DelegateLanes::new(
                DELEGATE,
                pool,
                vec![lane(DELEGATE, 1_250_000, 18_000_000_000)],
            )],
        }),
        pool_error: None,
        ..base(Mode::Loopback)
    };
    pin("status-loopback.json", &status);
}

#[test]
fn an_unreadable_store_keeps_its_shape() {
    let status = Status {
        pool_error: Some("the pull worker did not answer within 5 s".to_owned()),
        ..base(Mode::Loopback)
    };
    pin("status-pool-error.json", &status);
}
