use std::time::Duration;

use super::*;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

#[test]
fn reads_the_shared_active_set() {
    let active = Arc::new(RwLock::new(HashSet::from([nid(1)])));
    let set = ChainStakerSet::from_parts(Arc::clone(&active), RefreshTrigger::new());
    assert!(set.is_active(&nid(1)));
    assert_eq!(set.len(), 1);
    active.write().unwrap().insert(nid(2));
    assert!(set.is_active(&nid(2)));
    assert_eq!(set.active_nodes().len(), 2);
}

/// A lookup that misses asks the registry for an early re-read; a hit asks
/// nothing. The registry loop's rate limit is tested with the loop.
#[tokio::test(start_paused = true)]
async fn a_miss_requests_an_early_reread() {
    let trigger = RefreshTrigger::new();
    let set = ChainStakerSet::from_parts(
        Arc::new(RwLock::new(HashSet::from([nid(1)]))),
        trigger.clone(),
    );
    let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&runs);
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(crate::chain_events::refresh::run_refresh_loop(
        Duration::from_secs(300),
        Duration::from_secs(60),
        Some(trigger),
        shutdown.clone(),
        move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(())
        },
    ));
    assert!(set.is_active(&nid(1)));
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(
        runs.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a hit asks nothing"
    );
    assert!(!set.is_active(&nid(9)));
    assert!(!set.is_active(&nid(8)));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        runs.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "misses coalesce"
    );
    shutdown.cancel();
}
