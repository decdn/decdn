//! The runtime that owns an origin's pooled connections.
//!
//! A network origin ([`super::HttpOrigin`], [`super::S3Origin`]) shares one
//! keep-alive connection pool across every caller. hyper-util's pooled client
//! spawns each connection's dispatch task, the task that returns a connection
//! to the pool, and the pool's idle reaper onto whichever runtime polls the
//! request send. The node's serve-miss pull legs each run on their own
//! current-thread runtime, which drops when the leg returns. A connection a
//! pull leg opened on its own runtime would die with that runtime, and a
//! concurrent pull leg that reused the connection would lose its in-flight
//! request ("dispatch task is gone", #1673).
//!
//! [`IoRuntime`] captures the runtime an origin is built on, and every
//! request send runs there. The node builds its origins on its main runtime,
//! so no pooled connection dies with a pull leg's runtime. The response body
//! stays with the caller: hyper feeds it through a channel, so any runtime
//! can read it, and a reader that drops mid-body only closes that connection.

use std::future::Future;

use tokio::runtime::Handle;
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;

use crate::error::OriginPullError;

/// Handle to the runtime that runs an origin's request sends. See the
/// module docs.
#[derive(Debug, Clone)]
pub(crate) struct IoRuntime(Handle);

impl IoRuntime {
    /// Capture the runtime the caller runs on.
    ///
    /// # Errors
    ///
    /// Fails when the caller is not inside a tokio runtime.
    pub(crate) fn current() -> anyhow::Result<Self> {
        Handle::try_current().map(Self).map_err(|e| {
            anyhow::anyhow!(
                "a network origin must be built inside the tokio runtime that will own \
                 its connections: {e}"
            )
        })
    }

    /// Run `fut` on this runtime and wait for its output.
    ///
    /// The spawned task carries the caller's current span. Dropping the
    /// returned future aborts the task, so a request the caller stops waiting
    /// for stops too. Nothing here bounds the wait: a caller that needs a
    /// deadline wraps this future in its own timeout, which keeps the
    /// deadline on the caller's runtime even when this one stops polling.
    ///
    /// # Errors
    ///
    /// Both faults are [`OriginPullError::Permanent`], with the [`JoinError`]
    /// as the source:
    ///
    /// - The task panics. That is a bug, and it is also logged at error level.
    /// - The runtime cancels the task because it is shutting down. A shut-down
    ///   runtime cancels every later spawn too, so a retry cannot succeed.
    ///
    /// [`JoinError`]: tokio::task::JoinError
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Result<T, OriginPullError> {
        let task = AbortOnDropHandle::new(self.0.spawn(fut.instrument(tracing::Span::current())));
        task.await.map_err(|e| {
            let context = if e.is_panic() {
                tracing::error!(error = %e, "origin request task panicked");
                "the origin request task panicked"
            } else {
                "the origin request task was cancelled: the runtime that owns origin \
                 connections is shut down"
            };
            OriginPullError::Permanent(anyhow::Error::new(e).context(context))
        })
    }
}

#[cfg(test)]
mod tests {
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
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), io.run(async {})).await
            })
            .map_err(|_| anyhow::anyhow!("a send on a shut-down runtime hung"))?;
        match outcome {
            Err(OriginPullError::Permanent(_)) => Ok(()),
            Err(e @ OriginPullError::Transient(_)) => {
                anyhow::bail!("a shut-down runtime never recovers, so this is not transient: {e}")
            }
            Ok(()) => anyhow::bail!("a send on a shut-down runtime ran"),
        }
    }
}
