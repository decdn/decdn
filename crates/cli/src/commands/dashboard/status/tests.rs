//! The `/_status` contract with the page: a fully populated status, and one
//! with no gateway and no node, pinned to the JSON fixtures the page's tests
//! read (`crates/cli/dashboard/src/fixtures/dashboard-*.json`). A change to
//! the body fails here until the fixtures are rewritten with
//! `UPDATE_STATUS_FIXTURES=1`, and the page's `api.ts` must then match them.
//! The gateway section carries the gateway's own body, pinned by
//! `gateway::status`'s fixtures (`status-*.json`): here it is a stand-in
//! that names the fixture to splice in.

use std::path::{Path, PathBuf};

use decdn_common::admin::{
    BindingStatus, BucketStat, BuyerLaneSnapshot, BuyerPoolSnapshot, EarningsPeriod, LaneSnapshot,
    LastSettlement, PendingRedeem, RecordStoreHealth, RepublishHealth, RoutingHealth,
};

use super::super::super::activity::{BundleCounts, NodePaid, Paid, PullKind, PullState};
use super::*;

const STARTED: u64 = 1_793_000_000;
const NOW_MS: u64 = 1_793_000_600_000;
const OPERATOR: &str = "0xB2B2b2b2B2b2b2b2B2b2b2b2b2b2B2b2b2B2b2B2";
const POOL: &str = "0x0101010101010101010101010101010101010101010101010101010101010101";
const HASH: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

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

fn ok<T>(value: T) -> Part<T> {
    Part {
        updated_unix_ms: Some(NOW_MS),
        read: Read::Ok { value },
    }
}

fn period(from_unix: u64, net: u64) -> EarningsPeriod {
    EarningsPeriod {
        from_unix,
        net_micro_usdc: net,
        gross_micro_usdc: net + net / 9,
        bytes_settled: net * 1_000,
        settlements: 3,
    }
}

/// How a fixture's run ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Running,
    Done,
    Interrupted,
    Failed,
    GaveUp,
}

fn pull(started: u64, kind: PullKind, end: End) -> PullView {
    let finished_unix_ms = started + 42_000;
    let error = "no node holds the blob".to_owned();
    let state = match end {
        End::Running => PullState::Running,
        End::Done => PullState::Done { finished_unix_ms },
        End::Interrupted => PullState::Interrupted { finished_unix_ms },
        End::Failed => PullState::Failed {
            finished_unix_ms,
            error,
        },
        End::GaveUp => PullState::GaveUp {
            finished_unix_ms,
            error: "gave up: no verified byte for 2m".to_owned(),
        },
    };
    let ended = end != End::Running;
    PullView {
        record: PullRecord {
            uri: match kind {
                PullKind::Blob => format!("decdn:{HASH}"),
                PullKind::Bundle => format!("decdn:{HASH}/"),
                PullKind::Entry => format!("decdn:{HASH}/docs/index.html"),
            },
            kind,
            output: "/home/me/site".to_owned(),
            pid: 4242,
            started_unix_ms: started,
            updated_unix_ms: if ended {
                finished_unix_ms
            } else {
                started + 600
            },
            state,
            received_bytes: 734_003_200,
            expected_bytes: Some(1_073_741_824),
            files_started: 12,
            files_landed: 9,
            paid: Paid {
                amount_micro_usdc: 7_340,
                bytes: 736_100_352,
            },
            nodes: vec![NodePaid {
                provider: OPERATOR.to_owned(),
                paid: Paid {
                    amount_micro_usdc: 7_340,
                    bytes: 736_100_352,
                },
            }],
            bundle: (kind == PullKind::Bundle && ended).then_some(BundleCounts {
                fetched: 9,
                linked: 1,
                skipped: 2,
                excluded: 0,
                failed: 0,
                not_run: 0,
                downloaded: 734_003_200,
                resumed_bytes: 0,
                reused_bytes: 4_096,
            }),
        },
        stale: false,
    }
}

fn base(gateway: GatewayState, node: NodeSection) -> Status {
    Status {
        version: "0.3.0",
        now_unix_ms: NOW_MS,
        dashboard_built: true,
        started_unix: STARTED,
        pulls: PullsSection {
            dir: "/home/me/.decdn/activity/pulls".to_owned(),
            updated_unix_ms: Some(NOW_MS),
            active: Vec::new(),
            recent: Vec::new(),
            unreadable: 0,
            error: None,
        },
        gateway: GatewaySection {
            url: "http://127.0.0.1:8484/".to_owned(),
            updated_unix_ms: Some(NOW_MS),
            state: gateway,
        },
        node,
    }
}

fn node_waiting(state: NodeState) -> NodeSection {
    NodeSection {
        admin_url: "http://127.0.0.1:9191".to_owned(),
        metrics_url: "http://127.0.0.1:9090/metrics".to_owned(),
        state,
        health: Part::waiting(),
        status: Part::waiting(),
        metrics: Part::waiting(),
        lanes: Part::waiting(),
        earnings: Part::waiting(),
        pools: Part::waiting(),
        slashes: Part::waiting(),
    }
}

fn node_up() -> NodeSection {
    let node_id = "5f".repeat(32);
    NodeSection {
        state: NodeState::Up,
        health: ok(HealthResponse {
            node_id: node_id.clone(),
            uptime_s: 86_400,
            in_flight_streams: 3,
            binding: BindingStatus::Bound,
            bound_node_id: Some(node_id.clone()),
            registry_active: true,
        }),
        status: ok(StatusResponse {
            node_id,
            routing: RoutingHealth {
                total_peers: 7,
                non_empty_buckets: 2,
                buckets: vec![
                    BucketStat {
                        index: 250,
                        fill: 3,
                    },
                    BucketStat {
                        index: 255,
                        fill: 4,
                    },
                ],
                bucket_capacity: 20,
                refresh_interval_s: 300,
                last_refresh_us: Some(1_793_000_500_000_000),
            },
            known_stakers: 9,
            record_store: RecordStoreHealth {
                records: 1_204,
                capacity: 65_536,
            },
            republish: RepublishHealth {
                scheduled_records: 1_204,
            },
            chain_denied_origins: 0,
            operator_address: Some(OPERATOR.to_owned()),
        }),
        metrics: ok(NodeMetrics {
            snapshot: Snapshot {
                uptime_seconds: 86_400,
                active_connections: 5,
                dispatch_in_flight: 2,
                cache_hits: 9_000,
                cache_misses: 1_000,
                cache_bytes_returned: 52_428_800_000,
                unredeemed_usdc: 1_250_000,
                rpc_healthy: true,
            },
            rates: Some(SnapshotDelta {
                hits: 4.5,
                misses: 0.5,
                bytes: 2_097_152.0,
            }),
            hit_rate: Some(0.9),
        }),
        lanes: ok(LanesResponse {
            lanes: vec![LaneSnapshot {
                pool_id: POOL.to_owned(),
                counterparty: Some("0xAbaBabaBABaBAbAbABABaBABAbabABAbAbaBabab".to_owned()),
                voucher_signer: "0xCdCdCdCDCdcDcDCdCdCDCdCDCdcdcDcDcdcDCDcd".to_owned(),
                last_nonce: 812,
                outstanding_micro_usdc: 3_200,
                unredeemed_micro_usdc: 41_000,
                deposit_micro_usdc: 5_000_000,
                seconds_since_last_voucher: Some(12),
                settlement_eligible: true,
            }],
            redeem_threshold_micro_usdc: 100_000,
        }),
        earnings: ok(EarningsResponse {
            operator_address: Some(OPERATOR.to_owned()),
            ledger_since_unix: Some(1_790_000_000),
            last_24h: period(1_792_913_600, 120_000),
            last_7d: period(1_792_395_200, 910_000),
            since_ledger_start: period(1_790_000_000, 4_400_000),
            since: None,
            last_settlement: Some(LastSettlement {
                at_unix: 1_792_990_000,
                tx_hash: format!("0x{}", "ee".repeat(32)),
                block_number: 9_876_543,
                net_micro_usdc: 54_000,
            }),
            pending: Some(PendingRedeem {
                micro_usdc: 41_000,
                lanes: 1,
                stranded_micro_usdc: 0,
                stranded_lanes: 0,
                last_sweep_at_unix: Some(1_792_999_000),
                next_sweep_at_unix: Some(1_793_002_600),
            }),
        }),
        pools: ok(BuyerPoolsResponse {
            pools: vec![BuyerPoolSnapshot {
                pool_id: POOL.to_owned(),
                chain_id: 421_614,
                payment_pool: "0x1111111111111111111111111111111111111111".to_owned(),
                owner: OPERATOR.to_owned(),
                token: "0x2222222222222222222222222222222222222222".to_owned(),
                deposit_micro_usdc: 2_000_000,
                spent_micro_usdc: 310_000,
                lanes: vec![BuyerLaneSnapshot {
                    voucher_signer: "0xCdCdCdCDCdcDcDCdCdCDCdCDCdcdcDcDcdcDCDcd".to_owned(),
                    provider: "0x3333333333333333333333333333333333333333".to_owned(),
                    last_amount_micro_usdc: 310_000,
                    last_bytes_delivered: 31_000_000_000,
                }],
            }],
            skipped: Vec::new(),
        }),
        slashes: Part {
            updated_unix_ms: Some(NOW_MS),
            read: Read::Unconfigured {
                message: "slash detection is not configured".to_owned(),
            },
        },
        ..node_waiting(NodeState::Up)
    }
}

/// A host running everything: two live pulls (one whose heartbeat stopped),
/// three that ended, a loopback gateway, and a node with every read but
/// slashes.
#[test]
fn a_full_status_keeps_its_shape() {
    // The gateway's body is its own contract, pinned by the gateway's
    // fixtures; the page's tests splice those in here.
    let gateway = serde_json::json!({ "pinned_by": "status-loopback.json" });
    let mut status = base(GatewayState::Up { status: gateway }, node_up());
    let mut stopped = pull(NOW_MS - 120_000, PullKind::Blob, End::Running);
    stopped.stale = true;
    status.pulls.active = vec![
        pull(NOW_MS - 30_000, PullKind::Bundle, End::Running),
        stopped,
    ];
    status.pulls.recent = vec![
        pull(NOW_MS - 300_000, PullKind::Bundle, End::Done),
        pull(NOW_MS - 400_000, PullKind::Entry, End::Interrupted),
        pull(NOW_MS - 500_000, PullKind::Blob, End::Failed),
    ];
    status.pulls.unreadable = 1;
    pin("dashboard-full.json", &status);
}

/// A host with no gateway and no node: the pulls alone.
#[test]
fn a_status_with_nothing_running_keeps_its_shape() {
    let mut status = base(GatewayState::Absent, node_waiting(NodeState::Absent));
    status.node.health = Part {
        updated_unix_ms: Some(NOW_MS),
        read: Read::Error {
            message: "admin at http://127.0.0.1:9191 refused the connection".to_owned(),
        },
    };
    status.pulls.recent = vec![pull(NOW_MS - 300_000, PullKind::Blob, End::GaveUp)];
    pin("dashboard-alone.json", &status);
}

/// A node whose health call timed out while its other reads answered, a
/// gateway in error, and a pulls directory that cannot be listed.
#[test]
fn a_status_with_failing_sources_keeps_its_shape() {
    let message = "admin at http://127.0.0.1:9191 did not respond within 3000ms".to_owned();
    let mut node = node_up();
    node.state = NodeState::Error {
        message: message.clone(),
    };
    node.health = Part {
        updated_unix_ms: Some(NOW_MS),
        read: Read::Error { message },
    };
    let mut status = base(
        GatewayState::Error {
            message: "error sending request: connection reset by peer".to_owned(),
        },
        node,
    );
    status.pulls.error = Some(
        "cannot list /home/me/.decdn/activity/pulls: Permission denied (os error 13)".to_owned(),
    );
    pin("dashboard-failing.json", &status);
}

/// The gateway section's other states, as the page reads them.
#[test]
fn gateway_states_carry_their_tag() {
    let tag = |state| {
        serde_json::to_value(GatewaySection {
            url: "http://127.0.0.1:8484/".to_owned(),
            updated_unix_ms: None,
            state,
        })
        .unwrap()
    };
    assert_eq!(tag(GatewayState::Waiting)["state"], "waiting");
    assert_eq!(tag(GatewayState::Unauthorized)["state"], "unauthorized");
    let error = tag(GatewayState::Error {
        message: "HTTP 500".to_owned(),
    });
    assert_eq!(error["state"], "error");
    assert_eq!(error["message"], "HTTP 500");
}
