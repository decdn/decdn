//! A command's blocking state writes, run off the runtime thread in the order
//! they are queued.
//!
//! A fetch's lanes share the command's task (a `bundle pull` runs every entry
//! and lane as one future), so a durable write made inline there stops every
//! lane for as long as the disk takes (#2211). These writes are also order
//! sensitive across the whole command: a peer record's first open clears the
//! failure stamp a later fault set, and a voucher watermark rebase replaces
//! the lane row outright. One [`OrderedWrites`] per command therefore runs
//! every such write on the blocking pool, one at a time, in queue order.
//!
//! A worker runs on `spawn_blocking` only while writes are queued. It takes
//! them in order and exits when the queue is empty; the next queued write
//! starts a new one. The queue and the running flag share one lock, so a write
//! is never left queued with no worker to take it, and at most one worker runs
//! at a time. An idle command holds no blocking-pool thread. A worker is a
//! `spawn_blocking` writer, not a `tokio::spawn` of pull work, so a pull still
//! runs as one future.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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
    /// The queue state. A write that panicked leaves nothing half-done in it,
    /// so the poison is ignored.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The worker body: run every queued write in order, then exit.
    fn drain(&self) {
        let mut exit = WorkerExit {
            inner: self,
            clean: false,
        };
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
            write();
        }
        exit.clean = true;
    }
}

/// Clears the running flag when a worker unwinds out of a write, so the next
/// queued write starts a new worker instead of waiting on a dead one.
struct WorkerExit<'a> {
    inner: &'a Inner,
    clean: bool,
}

impl Drop for WorkerExit<'_> {
    fn drop(&mut self) {
        if !self.clean {
            self.inner.lock().running = false;
        }
    }
}

impl OrderedWrites {
    /// Queue `write` behind every write queued before it.
    ///
    /// Inside a Tokio runtime a blocking-pool worker runs it; outside one the
    /// queue drains on this thread before this returns.
    pub(crate) fn queue(&self, write: impl FnOnce() + Send + 'static) {
        let start = {
            let mut state = self.inner.lock();
            state.queue.push_back(Box::new(write));
            !std::mem::replace(&mut state.running, true)
        };
        if !start {
            return;
        }
        let inner = Arc::clone(&self.inner);
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(move || inner.drain())),
            Err(_) => inner.drain(),
        }
    }

    /// Queue `write` like [`Self::queue`], and return a receiver that resolves
    /// with its result once it ran. The write is queued before this returns,
    /// so its place in the order is fixed at the call, not at the first poll.
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

    /// Whether a worker is running.
    #[cfg(test)]
    fn running(&self) -> bool {
        self.inner.lock().running
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests {
    use std::sync::{Arc, Mutex};

    use super::OrderedWrites;

    /// Writes from separate submitters run in queue order, even while an
    /// earlier write is still running, and the worker exits once the queue is
    /// empty.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_run_in_queue_order_and_the_worker_exits_when_idle() {
        let writes = OrderedWrites::default();
        let log = Arc::new(Mutex::new(Vec::new()));
        let (release, hold) = std::sync::mpsc::channel::<()>();
        {
            let log = Arc::clone(&log);
            writes.queue(move || {
                hold.recv().unwrap();
                log.lock().unwrap().push(0);
            });
        }
        // A second submitter's writes, queued behind the held one.
        let other = writes.clone();
        for i in 1..=50 {
            let log = Arc::clone(&log);
            other.queue(move || log.lock().unwrap().push(i));
        }
        release.send(()).unwrap();
        writes.queue_awaitable(|| ()).await.unwrap();
        assert_eq!(*log.lock().unwrap(), (0..=50).collect::<Vec<_>>());
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while writes.running() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the worker exits once the queue is empty");
    }

    /// A write queued after the queue drained starts a new worker.
    #[tokio::test]
    async fn a_write_after_an_idle_spell_still_runs() {
        let writes = OrderedWrites::default();
        assert_eq!(writes.queue_awaitable(|| 1).await.unwrap(), 1);
        assert_eq!(writes.queue_awaitable(|| 2).await.unwrap(), 2);
    }
}
