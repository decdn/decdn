use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// Spawn a loop that counts its runs.
fn spawn_counter(
    interval: Duration,
    min_gap: Duration,
    trigger: Option<RefreshTrigger>,
) -> (Arc<AtomicUsize>, CancellationToken) {
    let runs = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let counted = Arc::clone(&runs);
    tokio::spawn(run_refresh_loop(
        interval,
        min_gap,
        trigger,
        shutdown.clone(),
        move || {
            counted.fetch_add(1, Ordering::SeqCst);
            std::future::ready(())
        },
    ));
    (runs, shutdown)
}

/// Let the spawned loop reach its next await at the current virtual time.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn runs_once_per_interval() {
    let (runs, shutdown) = spawn_counter(Duration::from_secs(300), Duration::from_secs(60), None);
    settle().await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the boot snapshot is the first read"
    );
    tokio::time::advance(Duration::from_secs(299)).await;
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(300)).await;
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    shutdown.cancel();
}

/// A burst of unknown-key requests costs one early re-read, and none before
/// `min_gap` has passed since the last read.
#[tokio::test(start_paused = true)]
async fn requests_are_rate_limited_to_one_per_gap() {
    let trigger = RefreshTrigger::new();
    let (runs, shutdown) = spawn_counter(
        Duration::from_secs(300),
        Duration::from_secs(60),
        Some(trigger.clone()),
    );
    settle().await;
    for _ in 0..10 {
        trigger.request();
    }
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 0, "held until the gap passes");
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the burst coalesced into one read"
    );

    // A request right after the read waits out a full gap again.
    trigger.request();
    tokio::time::advance(Duration::from_secs(59)).await;
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    shutdown.cancel();
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_the_loop() {
    let (runs, shutdown) = spawn_counter(Duration::from_secs(10), Duration::from_secs(1), None);
    shutdown.cancel();
    settle().await;
    tokio::time::advance(Duration::from_secs(100)).await;
    settle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}
