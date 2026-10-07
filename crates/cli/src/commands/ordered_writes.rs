//! A command's blocking state writes, run off the runtime thread in the order
//! they are queued.
//!
//! A fetch's lanes share the command's task (a `bundle pull` runs every entry
//! and lane as one future), so a durable write made inline there stops every
//! lane for as long as the disk takes (#2211). These writes are also order
//! sensitive across the whole command: an open that landed after a later
//! fault would clear that fault's stamp, and a voucher watermark rebase
//! replaces the lane row outright. One [`OrderedWrites`] per command
//! therefore runs a lane's open and fault peer records and its watermark
//! commits on the blocking pool, one at a time, in queue order. Other state
//! writes do not go through it: `spawn_harvest`'s `record_sample`,
//! `resolve_bootstrap`'s identity upserts, and the pool `record` /
//! `add_deposit` writes of an open or top-up.
//!
//! A worker runs on `spawn_blocking` only while writes are queued. It takes
//! them in order and exits when the queue is empty; the next queued write
//! starts a new one. The queue and the running flag share one lock, so a write
//! is never left queued with no worker to take it, and at most one worker runs
//! at a time. An idle command holds no blocking-pool thread. A worker is a
//! `spawn_blocking` writer, not a `tokio::spawn` of pull work, so a pull still
//! runs as one future. A write that panics is logged and the queue drains on,
//! so no write queued behind it is stranded. A worker the runtime drops
//! without running it, as a runtime that is shutting down does, drains the
//! queue on the thread that drops it.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// How long a command waits, before it returns, for the writes it queued
/// ([`OrderedWrites::settle`]).
pub(crate) const WRITE_DRAIN: Duration = Duration::from_secs(30);

/// One queued write.
type Write = Box<dyn FnOnce() + Send>;

/// A command-wide queue of blocking writes, run in queue order. Cloning shares
/// the queue.
#[derive(Clone, Default)]
pub(crate) struct OrderedWrites {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Write>,
    /// A worker is taking writes from `queue`.
    running: bool,
}

impl Inner {
    /// The queue state. No write runs under this lock, and each critical
    /// section leaves the queue whole, so a poisoned lock is safe to reuse.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The worker body: run every queued write in order, then exit. A write
    /// that panics is logged, and the next one runs.
    fn drain(&self) {
        loop {
            let next = {
                let mut state = self.lock();
                let next = state.queue.pop_front();
                if next.is_none() {
                    state.running = false;
                }
                next
            };
            let Some(write) = next else {
                break;
            };
            if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(write)) {
                let reason = panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("a non-string payload");
                tracing::error!("a queued state write panicked: {reason}");
            }
        }
    }
}

/// Drains the queue when it drops: inside the worker that runs it, or, when
/// the runtime drops the worker without running it, on the dropping thread.
struct DrainOnDrop(Arc<Inner>);

impl Drop for DrainOnDrop {
    fn drop(&mut self) {
        self.0.drain();
    }
}

impl OrderedWrites {
    /// Queue `write` behind every write queued before it.
    ///
    /// A running worker takes it in turn. With none running, this call starts
    /// one on the blocking pool. Outside a Tokio runtime, or when a runtime
    /// that is shutting down drops the new worker, the queue drains on this
    /// thread before this returns.
    pub(crate) fn queue(&self, write: impl FnOnce() + Send + 'static) {
        let start = {
            let mut state = self.inner.lock();
            state.queue.push_back(Box::new(write));
            !std::mem::replace(&mut state.running, true)
        };
        if !start {
            return;
        }
        let drain = DrainOnDrop(Arc::clone(&self.inner));
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(move || drop(drain))),
            Err(_) => drop(drain),
        }
    }

    /// Queue `write` like [`Self::queue`], and return a receiver that resolves
    /// with its result once it ran. The write is queued before this returns,
    /// so its place in the order is fixed at the call, not at the first poll.
    /// The receiver fails if the write panicked.
    pub(crate) fn queue_awaitable<T: Send + 'static>(
        &self,
        write: impl FnOnce() -> T + Send + 'static,
    ) -> tokio::sync::oneshot::Receiver<T> {
        let (done, landed) = tokio::sync::oneshot::channel();
        self.queue(move || {
            // The caller may have stopped waiting; the write still ran.
            let _ = done.send(write());
        });
        landed
    }

    /// Wait, for at most [`WRITE_DRAIN`], until every write queued so far has
    /// run. A command calls this before it returns, on the interrupted path
    /// too, so a drop guard's watermark write is durable before the process
    /// exits.
    pub(crate) async fn settle(&self) {
        let barrier = self.queue_awaitable(|| ());
        match tokio::time::timeout(WRITE_DRAIN, barrier).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => tracing::warn!(
                "the queued state writes did not finish; the next run may re-sign a stale \
                 voucher watermark"
            ),
            Err(_) => tracing::warn!(
                "the queued state writes were still running {WRITE_DRAIN:?} after the command \
                 ended; the next run may re-sign a stale voucher watermark"
            ),
        }
    }

    /// Whether a worker is running.
    #[cfg(test)]
    fn running(&self) -> bool {
        self.inner.lock().running
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests;
