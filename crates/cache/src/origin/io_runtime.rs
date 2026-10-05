//! The runtime that owns an origin's pooled connections.
//!
//! A network origin ([`super::HttpOrigin`], [`super::S3Origin`]) shares one
//! keep-alive connection pool across every caller. hyper spawns each pooled
//! connection's dispatch task, the task that returns a connection to the
//! pool, and the pool's idle reaper onto whichever runtime polls the request
//! send. The node's serve-miss pull legs run on per-serve current-thread
//! runtimes that drop when their serve ends. A connection a pull leg opened
//! on its own runtime would die with that runtime, and a concurrent serve
//! that reused the connection would lose its in-flight request ("dispatch
//! task is gone", #1673).
//!
//! [`IoRuntime`] captures the runtime an origin is built on, and every
//! request send runs there. The node builds its origins on its main runtime,
//! so each pooled connection lives as long as the node. The response body
//! stays with the caller: hyper feeds it through a channel, so any runtime
//! can read it, and a reader that drops mid-body only closes that connection.

use std::future::Future;

use tokio::runtime::Handle;
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
    /// for stops too.
    ///
    /// # Errors
    ///
    /// A task that panics fails as [`OriginPullError::Permanent`]. A task that
    /// the runtime cancels because it is shutting down fails as
    /// [`OriginPullError::Transient`].
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Result<T, OriginPullError> {
        let mut task = AbortOnDrop(self.0.spawn(fut.instrument(tracing::Span::current())));
        (&mut task.0).await.map_err(|e| {
            if e.is_panic() {
                OriginPullError::Permanent(anyhow::anyhow!("the origin request task panicked"))
            } else {
                OriginPullError::Transient(anyhow::anyhow!(
                    "the origin request task was cancelled by its runtime shutting down"
                ))
            }
        })
    }
}

/// Aborts the spawned task when dropped.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
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
        drop(tokio::time::timeout(std::time::Duration::from_millis(10), run).await);
        // The aborted task drops `tx`, which closes the channel.
        anyhow::ensure!(rx.await.is_err(), "the task outlived its caller");
        Ok(())
    }
}
