use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct Stub {
    seats: Mutex<Vec<(U256, Address)>>,
    reads: AtomicUsize,
}

impl Stub {
    fn new(seats: Vec<(U256, Address)>) -> Self {
        Self {
            seats: Mutex::new(seats),
            reads: AtomicUsize::new(0),
        }
    }
}

impl SeatReads for Arc<Stub> {
    async fn snapshot_block(&self) -> Result<u64> {
        Ok(7)
    }

    async fn seats_at(&self, _at: u64) -> Result<Vec<(U256, Address)>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.seats.lock().unwrap().clone())
    }
}

#[tokio::test]
async fn a_reseed_replaces_the_set() {
    let seated = SeatedOriginSet::from_seats([(U256::from(1), Address::repeat_byte(1))]);
    let stub = Arc::new(Stub::new(vec![(U256::from(2), Address::repeat_byte(2))]));
    reseed(&stub, &seated, &Metrics::new()).await;
    assert!(!seated.is_seated_in(U256::from(1), &Address::repeat_byte(1)));
    assert!(seated.is_seated_in(U256::from(2), &Address::repeat_byte(2)));
}

/// An attestation naming an operator the set does not hold pulls the next
/// re-read forward, at most once per gap; without one, the set waits out the
/// full interval.
#[tokio::test(start_paused = true)]
async fn an_unknown_origin_pulls_a_rate_limited_reseed() {
    let seated = Arc::new(SeatedOriginSet::new());
    let stub = Arc::new(Stub::new(vec![(U256::from(4), Address::repeat_byte(4))]));
    let shutdown = CancellationToken::new();
    tokio::spawn(refresh_task(
        Arc::clone(&stub),
        Arc::clone(&seated),
        Arc::new(Metrics::new()),
        shutdown.clone(),
    ));
    tokio::task::yield_now().await;
    for _ in 0..10 {
        assert!(!seated.is_seated_in(U256::from(4), &Address::repeat_byte(4)));
    }
    tokio::time::sleep(UNKNOWN_SEAT_REFRESH_GAP.saturating_sub(Duration::from_secs(1))).await;
    assert_eq!(stub.reads.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(stub.reads.load(Ordering::SeqCst), 1);
    assert!(seated.is_seated_in(U256::from(4), &Address::repeat_byte(4)));
    tokio::time::sleep(SEAT_REFRESH_INTERVAL).await;
    assert_eq!(stub.reads.load(Ordering::SeqCst), 2);
    shutdown.cancel();
}
