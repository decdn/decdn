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
