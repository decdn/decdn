//! The teardown helpers' own contract: [`support::shutdown`] and [`support::reap`]
//! are what keep every other test in this package from parking until the
//! `.config/nextest.toml` backstop, so their deadline, panic and cancellation
//! behavior is pinned here rather than inferred from the suites that use them.
//!
//! A separate target, not a `#[test]` inside `support`: that module is included by
//! each integration binary, so a test living in it would be collected and run once
//! per binary.
//!
//! `start_paused` throughout — the deadline paths assert on a 10s timeout that
//! auto-advances the instant every task is idle, so none of this costs wall clock.

#![allow(clippy::expect_used, clippy::panic)]

mod support;

use support::{reap, shutdown};

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
