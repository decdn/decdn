use std::time::Duration;

use super::*;

#[test]
fn current_outside_a_runtime_is_an_error() -> anyhow::Result<()> {
    anyhow::ensure!(IoRuntime::current().is_err(), "built outside a runtime");
    Ok(())
}

/// The future runs on the captured runtime, not the caller's.
#[test]
fn run_executes_on_the_captured_runtime() -> anyhow::Result<()> {
    let owner = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("io-owner")
        .enable_all()
        .build()?;
    let io = owner.block_on(async { IoRuntime::current() })?;
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let name =
        caller.block_on(io.run(async { std::thread::current().name().map(str::to_owned) }))?;
    anyhow::ensure!(name.as_deref() == Some("io-owner"), "ran on {name:?}");
    Ok(())
}

/// Dropping the caller's future aborts the spawned task.
#[tokio::test]
async fn dropping_run_aborts_the_task() -> anyhow::Result<()> {
    let io = IoRuntime::current()?;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let run = io.run(async move {
        let _tx = tx;
        std::future::pending::<()>().await;
    });
    drop(tokio::time::timeout(Duration::from_millis(10), run).await);
    // The aborted task drops `tx`, which closes the channel.
    anyhow::ensure!(rx.await.is_err(), "the task outlived its caller");
    Ok(())
}

/// A panicking task is a permanent fault that keeps the panic message.
#[tokio::test]
#[expect(clippy::panic, reason = "the test needs a task that panics")]
async fn a_panicking_task_is_permanent_and_keeps_its_message() -> anyhow::Result<()> {
    let io = IoRuntime::current()?;
    match io.run(async { panic!("origin-send-boom") }).await {
        Err(OriginPullError::Permanent(e)) => {
            let chain = format!("{e:#}");
            anyhow::ensure!(chain.contains("origin-send-boom"), "message lost: {chain}");
            Ok(())
        }
        Err(e @ OriginPullError::Transient(_)) => {
            anyhow::bail!("a panic is not worth retrying: {e}")
        }
        Ok(()) => anyhow::bail!("a panicking task returned Ok"),
    }
}

/// A shut-down owner runtime fails every send at once, as a permanent
/// fault: the retry loop must not wait out its budget on it.
#[test]
fn a_shut_down_owner_runtime_fails_fast_and_permanently() -> anyhow::Result<()> {
    let owner = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let io = owner.block_on(async { IoRuntime::current() })?;
    owner.shutdown_background();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = caller
        .block_on(async { tokio::time::timeout(Duration::from_secs(2), io.run(async {})).await })
        .map_err(|_| anyhow::anyhow!("a send on a shut-down runtime hung"))?;
    match outcome {
        Err(OriginPullError::Permanent(_)) => Ok(()),
        Err(e @ OriginPullError::Transient(_)) => {
            anyhow::bail!("a shut-down runtime never recovers, so this is not transient: {e}")
        }
        Ok(()) => anyhow::bail!("a send on a shut-down runtime ran"),
    }
}
