//! The periodic `eth_call` refresh loop every chain-state snapshot runs on.
//!
//! A snapshot is re-read on a fixed interval. A [`RefreshTrigger`] lets the
//! code that meets an unknown key (a `NodeId` the registry does not hold, an
//! origin the seat set does not hold) ask for an early re-read; the loop runs
//! at most one re-read per `min_gap`, so a flood of unknown keys costs one
//! snapshot per gap.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

/// A handle that asks a refresh loop for an early re-read. Cloneable and cheap;
/// requests coalesce into one re-read.
#[derive(Clone, Debug, Default)]
pub struct RefreshTrigger {
    notify: Arc<Notify>,
}

impl RefreshTrigger {
    /// A trigger no loop listens to yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask the loop for an early re-read. A request while one is pending adds
    /// nothing.
    pub fn request(&self) {
        self.notify.notify_one();
    }
}

/// Run `refresh` every `interval` after the snapshot taken at start, and early
/// on a [`RefreshTrigger`] request, but never sooner than `min_gap` after the
/// previous run. Returns once `shutdown` is cancelled; a run in progress is
/// dropped at its next await.
pub(crate) async fn run_refresh_loop<F, Fut>(
    interval: Duration,
    min_gap: Duration,
    trigger: Option<RefreshTrigger>,
    shutdown: CancellationToken,
    mut refresh: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut last = Instant::now();
    loop {
        let due = last + interval;
        let early = async {
            match &trigger {
                Some(trigger) => {
                    trigger.notify.notified().await;
                    sleep_until(last + min_gap).await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            () = sleep_until(due) => {}
            () = early => {}
        }
        last = Instant::now();
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            () = refresh() => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
