//! Integration test for the bucket-refresh clock (issue #741).
//!
//! `run_bucket_refresh` stamps a shared `AtomicU64` with the wall-clock
//! microseconds at which each refresh pass completes; `admin_v1_status`
//! reads it to report the routing table's network-wide "last refreshed"
//! time. The clock starts at 0 ("never refreshed") and must advance to a
//! plausible non-zero value after the first pass — even when the routing
//! table is empty (the pass is then a no-op, but the stamp still runs, so
//! the timestamp tracks "the refresh task is alive and ran at T").
//!
//! Uses an empty routing table so the pass performs no network I/O (it
//! returns before contacting any peer), keeping the test hermetic.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use decdn_node::dht::bucket_refresh::run_bucket_refresh;
use decdn_node::dht::routing::{NodeId, RoutingTable};
use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets};
use tokio::sync::oneshot;

/// Plausible wall-clock-µs window for the stamp sanity check: after
/// 2020-01-01, before ~year 2100. Guards against a stray small value
/// (e.g. a monotonic-instant or zero stamp slipping through).
const Y2020_US: u64 = 1_577_836_800_000_000;
const Y2100_US: u64 = 4_102_444_800_000_000;

async fn local_endpoint(secret_key: SecretKey) -> Endpoint {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(bind)
        .expect("bind_addr")
        .bind()
        .await
        .expect("bind")
}

#[tokio::test]
async fn bucket_refresh_stamps_clock_after_first_pass() {
    let secret_key = SecretKey::generate();
    let public = secret_key.public();
    let endpoint = local_endpoint(secret_key).await;

    // Empty routing table → each pass is a no-op (no peers to contact), but
    // the clock must still be stamped after the pass.
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *public.as_bytes(),
    ))));
    let clock = Arc::new(AtomicU64::new(0));
    let (stop_tx, stop_rx) = oneshot::channel::<()>();

    // Never stamped before the first tick fires.
    assert_eq!(clock.load(Ordering::Relaxed), 0);

    let join = tokio::spawn(run_bucket_refresh(
        endpoint,
        public,
        Arc::clone(&routing),
        stop_rx,
        // Short interval so the first pass completes quickly. The first
        // `interval` tick is burned at t=0; the first stamp lands ~interval
        // later.
        Duration::from_millis(50),
        Arc::clone(&clock),
    ));

    // Poll for the stamp (real time; the task uses a real ticker). Bound the
    // wait so a regression that drops the stamp fails instead of hanging.
    let mut stamped = 0;
    for _ in 0..100 {
        stamped = clock.load(Ordering::Relaxed);
        if stamped != 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        stamped != 0,
        "refresh clock was never stamped after a pass (still 0)"
    );

    // The stamp is wall-clock microseconds since the epoch — sanity-check
    // it is a plausible recent timestamp rather than a stray small value.
    assert!(
        (Y2020_US..Y2100_US).contains(&stamped),
        "stamp {stamped} is not a plausible wall-clock microsecond value"
    );

    let _ = stop_tx.send(());
    join.await.expect("refresh task joins cleanly");
}
