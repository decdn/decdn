//! The teardown helpers' own contract: [`support::shutdown`] and [`support::reap`]
//! are what keep every other test in this package from parking until the
//! `.config/nextest.toml` backstop, so their deadline, panic and cancellation
//! behavior is pinned here rather than inferred from the suites that use them.
//!
//! A separate target, not a `#[test]` inside `support`: that module is included by
//! each integration binary, so a test living in it would be collected and run once
//! per binary.
//!
//! `start_paused` for the task-side paths — they assert on behavior rather than
//! on `support::SHUTDOWN_TIMEOUT`'s value, and the virtual clock auto-advances
//! the instant every task is idle, so none of that costs wall clock.
//!
//! The close paths run on the REAL clock, and pay `CLOSE_DEADLINE` for it. A
//! paused clock auto-advances past a deadline as soon as the runtime idles,
//! which manufactures a breach whatever the endpoint is doing — so a paused
//! close test passes with the stall removed, which is the whole thing it is
//! supposed to be asserting.

#![allow(clippy::expect_used, clippy::panic)]

mod support;

use std::time::Duration;

use iroh::Endpoint;
use iroh::endpoint::Connection;
use support::{fresh_key, local_endpoint, reap, shutdown, shutdown_within};

/// How long the close tests give teardown. Real wall clock, so it is sized to
/// be waited out: orders of magnitude above a healthy loopback close, and far
/// below `support::SHUTDOWN_TIMEOUT`, which is sized against the drain cap
/// rather than for being sat through.
const CLOSE_DEADLINE: Duration = Duration::from_secs(2);

/// A bound on the dial, so a peer that never accepts fails these tests instead
/// of parking them until the `.config/nextest.toml` backstop.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// A task that ends on its own is reaped, not aborted, and its value comes back.
#[tokio::test(start_paused = true)]
async fn reap_returns_a_finished_task_value() {
    let value = reap("finished", tokio::spawn(async { 7u32 }))
        .await
        .expect("a task that returns must reap cleanly");
    assert_eq!(value, 7);
}

/// A parked task fails at the deadline instead of parking the caller with it.
#[tokio::test(start_paused = true)]
async fn reap_bounds_a_parked_task() {
    let task = tokio::spawn(std::future::pending::<()>());
    let handle = task.abort_handle();
    let err = reap("parked", task)
        .await
        .expect_err("a task that never finishes must not reap cleanly");
    assert!(
        err.to_string().contains("parked, not finishing"),
        "the error must name the deadline, got: {err}"
    );
    // `abort` only SCHEDULES cancellation, so give the runtime the poll that drops
    // the task's future before observing it — the same ordering `shutdown` relies
    // on when it reaps between the abort and the close.
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "reap must abort a parked task, or the next teardown pays the deadline again"
    );
}

/// A panic reaches the caller with the label attached.
#[tokio::test(start_paused = true)]
async fn reap_surfaces_a_panic() {
    let task = tokio::spawn(async { panic!("handler blew up") });
    let err = reap("panicky", task)
        .await
        .expect_err("a panicking task must not reap cleanly");
    assert!(
        err.to_string().contains("panicky"),
        "the error must carry the label, got: {err}"
    );
}

/// `shutdown`'s own abort is not a failure — the whole suite depends on this.
#[tokio::test(start_paused = true)]
async fn shutdown_does_not_fail_on_its_own_abort() {
    let task = tokio::spawn(std::future::pending::<()>());
    shutdown([task], [])
        .await
        .expect("a task cancelled by shutdown's own abort is not a failure");
}

/// A server task that panicked before teardown still fails the test.
#[tokio::test(start_paused = true)]
async fn shutdown_returns_a_task_panic() {
    let task = tokio::spawn(async { panic!("server blew up") });
    // Let it land before teardown, so this pins the "already panicked" case the
    // abort cannot take away.
    tokio::task::yield_now().await;
    let err = shutdown([task], [])
        .await
        .expect_err("a panicking server task must fail the test that spawned it");
    assert!(
        err.to_string().contains("did not exit cleanly"),
        "the error must name the task, got: {err}"
    );
}

/// The FIRST fault is the one reported: a later task's panic is usually a
/// consequence of it, and reporting the consequence buries the cause.
#[tokio::test(start_paused = true)]
async fn shutdown_keeps_the_first_fault() {
    let first = tokio::spawn(async { panic!("first") });
    let second = tokio::spawn(async { panic!("second") });
    tokio::task::yield_now().await;
    let err = shutdown([first, second], [])
        .await
        .expect_err("two panicking tasks must still fail");
    assert!(
        err.to_string().contains("server task 0"),
        "the first fault must be the reported one, got: {err}"
    );
}

/// Every task reaped and every endpoint closed: the report says so, and its
/// counts match the totals `shutdown` was handed.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_reports_clean_counts() -> anyhow::Result<()> {
    let task = tokio::spawn(std::future::pending::<()>());
    let (ep, _addr) = local_endpoint(fresh_key(), vec![]).await?;
    let report = shutdown([task], [&ep]).await?;
    assert!(
        report.is_clean(),
        "a quiet teardown must be clean: {report:?}"
    );
    assert_eq!(report.reaped, 1, "the one task must be reaped");
    assert_eq!(report.closed, 1, "the one endpoint must be closed");
    Ok(())
}

/// A close that blocks on a never-draining connection still yields `Ok`, with
/// the report short of its endpoint total — the breach line's data, returned
/// rather than only printed.
///
/// The connection is dialed on a throwaway runtime that is then dropped, which
/// strands the driver task quinn spawned on it. `Endpoint::close` normally
/// bounds itself on the connection's own close timer, but that timer is driven
/// by the same stranded task, so the connection reaches neither drained nor
/// timed out and the close waits indefinitely. That is the shape
/// `node_origin::abandon_drain` exists to wait out (#1675).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_reports_a_stalled_close() -> anyhow::Result<()> {
    let (ep, _conn) = connected_endpoint(ConnDriver::Stranded).await?;
    let report = shutdown_within(CLOSE_DEADLINE, [], [&ep]).await?;
    assert!(
        !report.is_clean(),
        "the stalled close must be reported as a breach, got: {report:?}"
    );
    assert_eq!(
        report.closed, 0,
        "the stalled endpoint must not count as closed"
    );
    Ok(())
}

/// The control for the test above: the SAME endpoint, peer and deadline, with
/// the connection's driver left running, closes cleanly.
///
/// Without it the breach test proves nothing — a breach the setup produced for
/// some unrelated reason would read as coverage of the stranded driver. This is
/// the test that fails if the stranding stops being what causes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_reports_a_live_close_as_clean() -> anyhow::Result<()> {
    let (ep, _conn) = connected_endpoint(ConnDriver::Live).await?;
    let report = shutdown_within(CLOSE_DEADLINE, [], [&ep]).await?;
    assert!(
        report.is_clean(),
        "a driven connection must close inside the deadline, got: {report:?}"
    );
    Ok(())
}

/// Which runtime drives the connection the endpoint under test holds at close.
#[derive(Clone, Copy)]
enum ConnDriver {
    /// Dialed on the test's own runtime, which goes on driving it.
    Live,
    /// Dialed on a throwaway runtime that is then dropped, stranding the
    /// driver: quinn spawns that task on whichever runtime is ambient at
    /// `connect` time, not on the one the endpoint was bound on.
    Stranded,
}

/// Bind an endpoint, dial a peer that holds the connection open, and return the
/// endpoint with the live `Connection`. The caller keeps the connection, so the
/// endpoint still tracks an unfinished one when it is closed.
///
/// The peer's accept task is detached and parks forever, holding its own
/// endpoint up for as long as the test needs it. A failed accept needs no
/// channel of its own: the handshake does not complete without one, so the dial
/// fails and carries the error.
///
/// # Errors
///
/// A bind failed, or the dial did not complete within `DIAL_TIMEOUT`.
async fn connected_endpoint(driver: ConnDriver) -> anyhow::Result<(Endpoint, Connection)> {
    const ALPN: &[u8] = b"cdn/stall/v1";

    let (ep, _addr) = local_endpoint(fresh_key(), vec![ALPN.to_vec()]).await?;
    let (peer_ep, peer_addr) = local_endpoint(fresh_key(), vec![ALPN.to_vec()]).await?;
    let peer_id = peer_ep.id();
    tokio::spawn(async move {
        if let Some(incoming) = peer_ep.accept().await
            && let Ok(connecting) = incoming.accept()
        {
            let _held = connecting.await;
            std::future::pending::<()>().await;
        }
    });

    let addr = iroh::EndpointAddr::new(peer_id).with_ip_addr(peer_addr);
    let conn = match driver {
        ConnDriver::Live => dial(&ep, addr, ALPN).await?,
        ConnDriver::Stranded => {
            let dialer = ep.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let conn = rt.block_on(dial(&dialer, addr, ALPN))?;
                // Dropping the runtime strands the driver task quinn spawned on
                // it, so the connection can never reach drained from here.
                drop(rt);
                Ok::<_, anyhow::Error>(conn)
            })
            .join()
            .map_err(|_| anyhow::anyhow!("the dialing thread panicked"))??
        }
    };
    Ok((ep, conn))
}

/// Dial `addr` under `DIAL_TIMEOUT`.
///
/// # Errors
///
/// The dial failed, or did not complete in time.
async fn dial(ep: &Endpoint, addr: iroh::EndpointAddr, alpn: &[u8]) -> anyhow::Result<Connection> {
    tokio::time::timeout(DIAL_TIMEOUT, ep.connect(addr, alpn))
        .await
        .map_err(|_| anyhow::anyhow!("the dial did not complete within {DIAL_TIMEOUT:?}"))?
        .map_err(|e| anyhow::anyhow!("dial: {e}"))
}
