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
mod tests {
    use std::sync::{Arc, Mutex};

    use super::OrderedWrites;

    /// The bound on every wait here, so a regression fails instead of hanging.
    const TEST_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

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
        tokio::time::timeout(TEST_WAIT, writes.queue_awaitable(|| ()))
            .await
            .expect("the queue drains")
            .unwrap();
        assert_eq!(*log.lock().unwrap(), (0..=50).collect::<Vec<_>>());
        tokio::time::timeout(TEST_WAIT, async {
            while writes.running() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the worker exits once the queue is empty");
    }

    /// A write that panics strands nothing queued behind it: the awaited write
    /// behind it still lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::panic)] // the write under test panics on purpose
    async fn a_panicking_write_does_not_strand_the_writes_behind_it() {
        let writes = OrderedWrites::default();
        let (release, hold) = std::sync::mpsc::channel::<()>();
        writes.queue(move || {
            let _ = hold.recv();
            panic!("a write that fails");
        });
        let behind = writes.queue_awaitable(|| 7);
        release.send(()).unwrap();
        let landed = tokio::time::timeout(std::time::Duration::from_secs(3), behind)
            .await
            .expect("the write behind the panic runs");
        assert_eq!(landed.unwrap(), 7);
    }

    /// A runtime that is shutting down drops a new worker without running it;
    /// the write still lands, on the queueing thread.
    #[test]
    fn a_worker_the_runtime_drops_still_drains_the_queue() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        runtime.shutdown_background();
        let _entered = handle.enter();
        let writes = OrderedWrites::default();
        let ran = Arc::new(Mutex::new(false));
        {
            let ran = Arc::clone(&ran);
            writes.queue(move || *ran.lock().unwrap() = true);
        }
        assert!(*ran.lock().unwrap(), "the dropped worker drained the queue");
        assert!(!writes.running());
    }

    /// A write queued after the queue drained starts a new worker.
    #[tokio::test]
    async fn a_write_after_an_idle_spell_still_runs() {
        let writes = OrderedWrites::default();
        for n in [1, 2] {
            let landed = tokio::time::timeout(TEST_WAIT, writes.queue_awaitable(move || n))
                .await
                .expect("the write runs");
            assert_eq!(landed.unwrap(), n);
        }
    }

    /// Many submitters racing the worker's exit: every write runs, none is
    /// left queued with no worker to take it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_submitters_never_strand_a_write() {
        let writes = OrderedWrites::default();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut submitters = Vec::new();
        for _ in 0..16 {
            let writes = writes.clone();
            let count = Arc::clone(&count);
            submitters.push(tokio::spawn(async move {
                for _ in 0..200 {
                    let count = Arc::clone(&count);
                    writes.queue(move || {
                        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    });
                    tokio::task::yield_now().await;
                }
                let last = writes.queue_awaitable(|| ());
                tokio::time::timeout(std::time::Duration::from_secs(2), last)
                    .await
                    .expect("a submitter's last write runs")
                    .unwrap();
            }));
        }
        for submitter in submitters {
            submitter.await.unwrap();
        }
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 16 * 200);
    }
}
