//! Two-endpoint integration tests for [`LimitedHandler`].
//!
//! Issue #433: a connection flood on a foreign ALPN (today the iroh-gossip
//! ALPN) must be gated by `ConnectionLimiter` so the global concurrency cap
//! holds network-wide, not just on the probe ALPN. The tests here:
//!
//! - exercise the global-cap rejection path through the wrapper,
//! - exercise the per-source rate-limit rejection path through the wrapper,
//! - prove the wrapper releases its permit when the inner handler returns
//!   `Err(AcceptError)` (otherwise a single faulty handler would permanently
//!   pin a slot).

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use decdn_common::config::ResolvedSecurity;
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::limited::LimitedHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::APP_ERR_RATE_LIMITED;
use iroh::endpoint::{ApplicationClose, Connection, ConnectionError, VarInt, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use tokio::sync::Notify;

const TEST_ALPN: &[u8] = b"test/limited/v1";

/// Inner handler whose `accept` parks on a `Notify` until the test releases
/// it. The `accepts` counter lets the test prove the wrapper short-circuited
/// without ever invoking the inner handler.
#[derive(Debug, Clone)]
struct ParkingHandler {
    accepts: Arc<AtomicUsize>,
    accept_started: Arc<Notify>,
    release: Arc<Notify>,
}

impl ParkingHandler {
    fn new() -> Self {
        Self {
            accepts: Arc::new(AtomicUsize::new(0)),
            accept_started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

impl ProtocolHandler for ParkingHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.accepts.fetch_add(1, Ordering::SeqCst);
        // Signal the test that we're holding the permit.
        self.accept_started.notify_one();
        // Park until the test releases us — the limiter slot is held the
        // whole time, so a concurrent client must observe a global-cap
        // rejection.
        self.release.notified().await;
        // Close cleanly so the client sees a normal teardown.
        conn.close(0u32.into(), b"test-done");
        Ok(())
    }
}

fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

/// Bind a loopback-only iroh `Endpoint` with relay disabled, so the test is
/// fully offline-deterministic. Returns the endpoint plus the resolved
/// loopback `SocketAddr` to dial it on.
async fn local_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, SocketAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(bind)
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind: {e}"))?;
    let addr = ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let addr = match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };
    Ok((ep, addr))
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_second_connection_past_global_cap() -> anyhow::Result<()> {
    // Strict global cap = 1. Per-source layer is wide open so this test
    // isolates the global-semaphore path.
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: 1,
        per_source_rate_per_sec: 1e9,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(&metrics)));

    let inner = ParkingHandler::new();
    let wrapper = LimitedHandler::new(inner.clone(), Arc::clone(&limiter));

    // Server endpoint + iroh Router with the wrapper attached. Using Router
    // (rather than a hand-rolled accept loop) is what the production wiring
    // does, so the test exercises the same path.
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![TEST_ALPN.to_vec()]).await?;
    let router = Router::builder(server_ep.clone())
        .accept(TEST_ALPN, wrapper)
        .spawn();

    // Client 1 — must succeed and park inside the inner handler.
    let (client1_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn1 = client1_ep
        .connect(target.clone(), TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client1 connect: {e}"))?;

    // Wait for the inner handler to actually be holding the permit. Without
    // this barrier, client 2 may race in before client 1's permit is
    // accounted for.
    tokio::time::timeout(Duration::from_secs(5), inner.accept_started.notified())
        .await
        .map_err(|_| anyhow::anyhow!("inner accept never fired for client1"))?;
    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        1,
        "inner.accept must have fired exactly once for client1"
    );

    // Client 2 — global cap exhausted; the wrapper must reject before
    // delegating to the inner handler.
    let (client2_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn2 = client2_ep
        .connect(target, TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client2 connect: {e}"))?;

    // Observe the close — must carry the `0x10 RATE_LIMITED` app error code
    // delivered as a connection-level `CONNECTION_CLOSE`.
    let expected = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    let close = tokio::time::timeout(Duration::from_secs(5), conn2.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client2 conn never closed"))?;
    match close {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. })
            if error_code == expected => {}
        other => anyhow::bail!("expected RATE_LIMITED close, got {other:?}"),
    }

    // The inner handler must NOT have observed client 2 — that is the whole
    // point of the wrapper. (Metrics-counter assertions live in the
    // `dispatch::tests` module, which has crate-private access to
    // `Metrics::encode`; the close-code + counter assertions here are
    // sufficient to prove the wrapper short-circuited.)
    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        1,
        "inner.accept must not fire for the rejected client2"
    );

    // Release the inner handler so client 1's task can return and the
    // permit is freed before tear-down.
    inner.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), conn1.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client1 failed to close cleanly within 5s"))?;

    // Tear down. `Router::shutdown` aborts in-flight handlers after
    // `ProtocolHandler::shutdown` returns; our `ParkingHandler::shutdown`
    // is the trait default (no-op) so this returns promptly. Propagate the
    // error so a regression where shutdown times out or an inner handler's
    // `shutdown` panics would fail the test rather than be swallowed.
    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router.shutdown: {e}"))?;
    client1_ep.close().await;
    client2_ep.close().await;
    server_ep.close().await;
    Ok(())
}

/// Inner handler whose `accept` immediately returns `Err`. Used to prove the
/// wrapper releases its permit on the inner-`Err` path (otherwise a single
/// faulty inner handler would permanently pin a slot in the global cap).
#[derive(Debug, Clone, Default)]
struct FailingHandler {
    accepts: Arc<AtomicUsize>,
}

impl ProtocolHandler for FailingHandler {
    async fn accept(&self, _conn: Connection) -> Result<(), AcceptError> {
        self.accepts.fetch_add(1, Ordering::SeqCst);
        Err(AcceptError::from_err(std::io::Error::other(
            "intentional test failure",
        )))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn permit_releases_when_inner_returns_err() -> anyhow::Result<()> {
    // Cap = 1. If the wrapper failed to drop the permit on the inner-`Err`
    // path, the second connect would be rejected with RATE_LIMITED.
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: 1,
        per_source_rate_per_sec: 1e9,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(&metrics)));

    let inner = FailingHandler::default();
    let wrapper = LimitedHandler::new(inner.clone(), Arc::clone(&limiter));

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![TEST_ALPN.to_vec()]).await?;
    let router = Router::builder(server_ep.clone())
        .accept(TEST_ALPN, wrapper)
        .spawn();
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Reuse one client endpoint across iterations. Endpoint setup (UDP bind
    // + keygen) dominates this loop's wall-clock; measured ~3x speedup
    // hoisting it out of the per-iteration body.
    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;

    // Three sequential connects. Each must reach the inner handler; if the
    // permit leaked on `Err`, only the first would. Waiting on
    // `conn.closed()` between iterations serializes the test against the
    // server-side permit drop — without it, iteration N+1's connect could
    // race iteration N's permit release. (This is a 3-iteration sequencing
    // test, not a flood: the "no `conn.closed()` wait under rejection"
    // guideline that protects the limiter's memory-pressure path doesn't
    // apply here.)
    for attempt in 1..=3u32 {
        let conn = client_ep
            .connect(target.clone(), TEST_ALPN)
            .await
            .map_err(|e| anyhow::anyhow!("attempt {attempt} connect: {e}"))?;
        tokio::time::timeout(Duration::from_secs(5), conn.closed())
            .await
            .map_err(|_| anyhow::anyhow!("attempt {attempt}: connection never closed within 5s"))?;
    }

    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        3,
        "all three sequential accepts must reach the inner handler — \
         a smaller value means the permit leaked on the Err path"
    );

    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router.shutdown: {e}"))?;
    client_ep.close().await;
    server_ep.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn per_source_rejection_routes_through_wrapper() -> anyhow::Result<()> {
    // Wide-open global cap; tight per-source layer (burst=1, very slow
    // refill) isolates the per-source rejection path through the wrapper.
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 0.001,
        per_source_burst: 1,
        max_tracked_sources: 16,
    };
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(&metrics)));

    let inner = ParkingHandler::new();
    let wrapper = LimitedHandler::new(inner.clone(), Arc::clone(&limiter));

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![TEST_ALPN.to_vec()]).await?;
    let router = Router::builder(server_ep.clone())
        .accept(TEST_ALPN, wrapper)
        .spawn();
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Both clients dial from 127.0.0.1, so they share the per-source bucket
    // for `127.0.0.1`. Burst=1 means the second connection must reject.
    let (client1_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn1 = client1_ep
        .connect(target.clone(), TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client1 connect: {e}"))?;
    tokio::time::timeout(Duration::from_secs(5), inner.accept_started.notified())
        .await
        .map_err(|_| anyhow::anyhow!("inner accept never fired for client1"))?;
    assert_eq!(inner.accepts.load(Ordering::SeqCst), 1);

    let (client2_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn2 = client2_ep
        .connect(target, TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client2 connect: {e}"))?;

    let expected = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    let close = tokio::time::timeout(Duration::from_secs(5), conn2.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client2 conn never closed"))?;
    match close {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. })
            if error_code == expected => {}
        other => anyhow::bail!("expected RATE_LIMITED close, got {other:?}"),
    }
    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        1,
        "inner.accept must not fire for the per-source-rejected client2"
    );

    inner.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), conn1.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client1 failed to close cleanly within 5s"))?;
    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router.shutdown: {e}"))?;
    client1_ep.close().await;
    client2_ep.close().await;
    server_ep.close().await;
    Ok(())
}

/// Inner handler that mimics iroh-gossip's `accept` shape: returns `Ok`
/// after handing the `Connection` off to an internally-spawned task. Used
/// to prove the wrapper's permit-holding watcher correctly bounds in-flight
/// connections even when `inner.accept` itself returns fast.
#[derive(Debug, Clone)]
struct ReturnFastHandler {
    accepts: Arc<AtomicUsize>,
    spawned: Arc<Notify>,
    release: Arc<Notify>,
}

impl ReturnFastHandler {
    fn new() -> Self {
        Self {
            accepts: Arc::new(AtomicUsize::new(0)),
            spawned: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

impl ProtocolHandler for ReturnFastHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.accepts.fetch_add(1, Ordering::SeqCst);
        let spawned = self.spawned.clone();
        let release = self.release.clone();
        // Hand the connection off to a detached task and return immediately,
        // mirroring how `Gossip::accept` enqueues into its actor's mpsc and
        // returns before the connection has been driven.
        tokio::spawn(async move {
            spawned.notify_one();
            release.notified().await;
            conn.close(0u32.into(), b"test-done");
        });
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_second_connection_for_return_fast_inner() -> anyhow::Result<()> {
    // Strict global cap = 1. The inner handler returns `Ok` from `accept`
    // before its spawned task has closed the connection — so if the wrapper
    // dropped the permit at `inner.accept`-return (the bug fixed by the
    // watcher), client 2 would NOT observe a RATE_LIMITED close.
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: 1,
        per_source_rate_per_sec: 1e9,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(&metrics)));

    let inner = ReturnFastHandler::new();
    let wrapper = LimitedHandler::new(inner.clone(), Arc::clone(&limiter));

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![TEST_ALPN.to_vec()]).await?;
    let router = Router::builder(server_ep.clone())
        .accept(TEST_ALPN, wrapper)
        .spawn();

    let (client1_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn1 = client1_ep
        .connect(target.clone(), TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client1 connect: {e}"))?;

    // `spawned` fires after the wrapper has reached its `tokio::spawn(...)`
    // call, so the watcher owns the permit by the time we proceed.
    tokio::time::timeout(Duration::from_secs(5), inner.spawned.notified())
        .await
        .map_err(|_| anyhow::anyhow!("inner.accept never spawned its task for client1"))?;
    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        1,
        "inner.accept must have fired exactly once for client1"
    );

    // Client 2 — global cap exhausted. The wrapper must reject before
    // delegating to the inner handler.
    let (client2_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn2 = client2_ep
        .connect(target, TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client2 connect: {e}"))?;

    let expected = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    let close = tokio::time::timeout(Duration::from_secs(5), conn2.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client2 conn never closed"))?;
    match close {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. })
            if error_code == expected => {}
        other => anyhow::bail!("expected RATE_LIMITED close, got {other:?}"),
    }
    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        1,
        "inner.accept must not fire for the rejected client2 — \
         a value of 2 means the permit was dropped at inner.accept-return \
         (the pre-watcher bug)"
    );

    // Release inner's task #1; it closes conn1 on the server side. Both
    // the server-side watcher's clone and the client's `conn1` observe the
    // close; the server-side `closed()` resolves before the close frame
    // reaches the client, so by the time `conn1.closed()` resolves below
    // the watcher has already dropped its permit.
    inner.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), conn1.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client1 failed to close cleanly within 5s"))?;

    // Client 3 — must now succeed, because the watcher released its permit
    // when conn1 closed. A future regression that detaches the permit
    // (`mem::forget`) or otherwise leaks it would not be caught by the
    // earlier RATE_LIMITED assertion alone.
    let (client3_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target3 = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn3 = client3_ep
        .connect(target3, TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client3 connect: {e}"))?;
    tokio::time::timeout(Duration::from_secs(5), inner.spawned.notified())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "inner.accept never spawned its task for client3 — \
                                      watcher likely failed to release the permit"
            )
        })?;
    assert_eq!(
        inner.accepts.load(Ordering::SeqCst),
        2,
        "client3 must reach the inner handler after conn1 closed"
    );

    // Cleanup task #2 so teardown is deterministic.
    inner.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), conn3.closed())
        .await
        .map_err(|_| anyhow::anyhow!("client3 failed to close cleanly within 5s"))?;

    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router.shutdown: {e}"))?;
    client1_ep.close().await;
    client2_ep.close().await;
    client3_ep.close().await;
    server_ep.close().await;
    Ok(())
}

/// `Router::shutdown` triggers `endpoint.close()`, which must fire every
/// pending `Connection::closed()` future — including the wrapper's
/// detached watcher. If a future iroh release ever changed that contract,
/// the watcher would hang on `closed().await` and the permit would never
/// drop, silently leaking the global-cap slot across reload cycles.
#[tokio::test(flavor = "multi_thread")]
async fn router_shutdown_releases_parked_watcher_permit() -> anyhow::Result<()> {
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: 1,
        per_source_rate_per_sec: 1e9,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(&metrics)));

    let inner = ReturnFastHandler::new();
    let wrapper = LimitedHandler::new(inner.clone(), Arc::clone(&limiter));

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![TEST_ALPN.to_vec()]).await?;
    let router = Router::builder(server_ep.clone())
        .accept(TEST_ALPN, wrapper)
        .spawn();

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let _conn = client_ep
        .connect(target, TEST_ALPN)
        .await
        .map_err(|e| anyhow::anyhow!("client connect: {e}"))?;
    tokio::time::timeout(Duration::from_secs(5), inner.spawned.notified())
        .await
        .map_err(|_| anyhow::anyhow!("inner.accept never spawned its task for client"))?;

    // Sanity: the permit is currently held by the watcher.
    assert!(
        limiter.acquire_for_test(None).is_err(),
        "global cap should be exhausted while the watcher holds the permit"
    );

    // Shut down WITHOUT releasing the inner handler. `endpoint.close()`
    // must fire the watcher's pending `closed().await` so the permit drops.
    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router.shutdown: {e}"))?;
    server_ep.close().await;

    // Poll until the permit becomes available. router.shutdown returning is
    // not synchronous with the watcher's drop (the watcher is detached), so
    // a brief window of `Err(GlobalFull)` is acceptable. The Permit from a
    // successful acquire_for_test drops on the boolean eval, so the loop is
    // self-cleaning.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if limiter.acquire_for_test(None).is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("permit never released after Router::shutdown"))?;

    client_ep.close().await;
    Ok(())
}
