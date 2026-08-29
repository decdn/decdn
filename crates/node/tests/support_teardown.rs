//! The teardown helpers' own contract: [`support::shutdown`] and [`support::reap`]
//! are what keep every other test in this package from parking until the
//! `.config/nextest.toml` backstop, so their deadline, panic and cancellation
//! behavior is pinned here rather than inferred from the suites that use them.
//!
//! A separate target, not a `#[test]` inside `support`: that module is included by
//! each integration binary, so a test living in it would be collected and run once
//! per binary.
//!
//! `start_paused` throughout — the deadline paths assert on behavior rather than on
//! `support::SHUTDOWN_TIMEOUT`'s value, and the virtual clock auto-advances the
//! instant every task is idle, so none of this costs wall clock.

#![allow(clippy::expect_used, clippy::panic)]

mod support;

use iroh::Endpoint;
use support::{fresh_key, local_endpoint, reap, shutdown};

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

/// The report's counters match what the docstring claims: every task reaped and
/// every endpoint closed on a clean teardown.
#[tokio::test(start_paused = true)]
async fn shutdown_reports_clean_counts() -> anyhow::Result<()> {
    let task = tokio::spawn(std::future::pending::<()>());
    let ep = local_endpoint(fresh_key(), vec![]).await?;
    let report = shutdown([task], [&ep.0]).await?;
    assert_eq!(report.reaped, 1, "the one task must be reaped");
    assert_eq!(report.closed, 1, "the one endpoint must be closed");
    Ok(())
}

/// A close that blocks on a never-draining connection still yields `Ok`, with
/// the report showing `closed < M` — the breach line's data, returned rather
/// than only printed.
///
/// The endpoint under test DIALS OUT and its connection driver lives on a
/// throwaway runtime; dropping that runtime strands the driver, so the
/// connection can never reach drained and `Endpoint::close` waits on it
/// forever — the same shape `abandon_drain` exists to wait out. `shutdown`
/// then hits its deadline and reports the shortfall instead of failing.
///
/// Every live socket driver runs on its own thread, not on this paused
/// runtime: a driver doing real socket I/O here would hold the virtual clock
/// instead of letting it reach the deadline.
#[tokio::test(start_paused = true)]
async fn shutdown_reports_a_stalled_close() -> anyhow::Result<()> {
    let (e_ep, _) = driven_endpoint(fresh_key(), vec![b"cdn/stall/v1".to_vec()])?;
    let (peer_ep, peer_addr) = driven_endpoint(fresh_key(), vec![b"cdn/stall/v1".to_vec()])?;
    let peer_id = peer_ep.id();

    // Peer accepts one connection and holds it forever, on its own runtime.
    let accept = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            if let Some(incoming) = peer_ep.accept().await
                && let Ok(connecting) = incoming.accept()
            {
                let _ = connecting.await;
                std::future::pending::<()>().await;
            }
        });
        Ok::<_, anyhow::Error>(())
    });

    // The endpoint-under-test dials out through a clone on a throwaway
    // runtime, then that runtime is dropped to strand the connection's
    // driver. The connection is kept alive so the endpoint still tracks it.
    let e_clone = e_ep.clone();
    let _conn = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let conn = rt.block_on(async {
            e_clone
                .connect(
                    iroh::EndpointAddr::new(peer_id).with_ip_addr(peer_addr),
                    b"cdn/stall/v1",
                )
                .await
        })?;
        drop(rt);
        Ok::<_, anyhow::Error>(conn)
    })
    .join()
    .map_err(|_| anyhow::anyhow!("connection thread panicked"))??;

    let report = shutdown([], [&e_ep]).await?;
    assert!(
        report.closed < 1,
        "the stalled close must be reported as a breach, got closed = {}",
        report.closed
    );
    let _ = accept;
    Ok(())
}

/// Bind an endpoint whose socket driver is driven on its own thread forever, so
/// no live driver runs on the caller's (possibly paused) runtime.
///
/// Returns the endpoint and its loopback address. The driver thread runs the
/// runtime with `block_on(pending())` for the lifetime of the program, so the
/// endpoint keeps working for as long as the test needs it.
fn driven_endpoint(
    key: iroh::SecretKey,
    alpn: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, std::net::SocketAddr)> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result: anyhow::Result<(Endpoint, std::net::SocketAddr)> =
            rt.block_on(local_endpoint(key, alpn));
        let ok = result.is_ok();
        let _ = tx.send(result);
        if ok {
            rt.block_on(std::future::pending::<()>());
        }
        Ok::<_, anyhow::Error>(())
    });
    rx.recv()?
}
