//! A background task's `CancellationToken` paired with its `JoinHandle` in one
//! type, so cancel-before-join is structural rather than call-site discipline.
//!
//! The receipt writer ([`crate::receipt_log::spawn_receipt_writer`]) and the
//! warming-credit aggregator
//! ([`crate::warming_allowance::spawn_warming_creditor`]) are each a drain loop
//! whose queue senders live inside long-lived holders such as
//! [`crate::handlers::client::ClientHandler`]. Awaiting such a task's bare
//! `JoinHandle` without first cancelling its token hangs until every sender
//! drops — which for those holders is never. [`StopHandle`] carries the token
//! and the handle as one value, and [`StopHandle::shutdown`] — the only way to
//! join the task — always cancels before it awaits.

use std::future::Future;

use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// A spawned background task and the `CancellationToken` that stops it.
///
/// [`StopHandle::spawn`] hands the task the same token the handle stores, so
/// the pairing holds by construction. Dropping the handle detaches the task
/// (like dropping a bare `JoinHandle`); stop it with [`StopHandle::shutdown`]
/// instead.
#[derive(Debug)]
pub struct StopHandle {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

impl StopHandle {
    /// Spawn `task` onto the runtime with a fresh stop token. The closure
    /// receives the token this handle later cancels, so the task observes
    /// exactly the signal [`Self::cancel`] and [`Self::shutdown`] fire.
    pub fn spawn<F, Fut>(task: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(task(shutdown.clone()));
        Self { shutdown, handle }
    }

    /// Signal the task to stop without awaiting it. Idempotent —
    /// [`Self::shutdown`] cancels again as a no-op. Use this to start a drain
    /// early and overlap it with other teardown work before the final
    /// `shutdown().await`.
    pub fn cancel(&self) {
        self.shutdown.cancel();
    }

    /// Last-resort abort for a drain that overruns its deadline: dropping a
    /// timed-out [`Self::shutdown`] future merely detaches the task, so the
    /// caller takes this handle first and fires it when the timeout lands.
    #[must_use]
    pub fn abort_handle(&self) -> AbortHandle {
        self.handle.abort_handle()
    }

    /// Stop the task and await it: cancel the token, then join. The cancel is
    /// unconditional and comes first, so this cannot hang on queue senders
    /// that outlive shutdown the way a bare `JoinHandle` await can.
    pub async fn shutdown(self) -> Result<(), JoinError> {
        self.shutdown.cancel();
        self.handle.await
    }
}
