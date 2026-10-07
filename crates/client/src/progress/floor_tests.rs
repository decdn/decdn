use super::*;

fn cfg(floor_bps: u64) -> FloorConfig {
    FloorConfig {
        window: Duration::from_secs(20),
        floor_bps,
    }
}

#[tokio::test(start_paused = true)]
async fn healthy_throughput_never_stalls() {
    let counter = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut f = ThroughputFloor::new(cfg(4096), Arc::clone(&counter), t0);
    for s in 1..=60u64 {
        counter.store(100 * 1024 * s, Ordering::Relaxed);
        let v = f.evaluate(t0 + Duration::from_secs(s));
        assert_eq!(v, FloorVerdict::Ok, "stalled at {s}s");
    }
}

#[tokio::test(start_paused = true)]
async fn slow_drip_below_floor_stalls_after_window() {
    let counter = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut f = ThroughputFloor::new(cfg(4096), Arc::clone(&counter), t0);
    let mut verdict = FloorVerdict::Ok;
    for s in 1..=25u64 {
        counter.store(100 * s, Ordering::Relaxed);
        verdict = f.evaluate(t0 + Duration::from_secs(s));
    }
    assert_eq!(verdict, FloorVerdict::Stalled);
}

#[tokio::test(start_paused = true)]
async fn full_wedge_stalls_with_floor_zero() {
    let counter = Arc::new(AtomicU64::new(1)); // one byte then silence
    let t0 = Instant::now();
    let mut f = ThroughputFloor::new(cfg(0), Arc::clone(&counter), t0);
    let mut verdict = FloorVerdict::Ok;
    for s in 1..=21u64 {
        verdict = f.evaluate(t0 + Duration::from_secs(s));
    }
    assert_eq!(verdict, FloorVerdict::Stalled);
}

#[tokio::test(start_paused = true)]
async fn payment_pause_is_not_charged_to_sender() {
    let counter = Arc::new(AtomicU64::new(1_000_000));
    let t0 = Instant::now();
    let mut f = ThroughputFloor::new(cfg(4096), Arc::clone(&counter), t0);
    for s in 1..=5u64 {
        counter.store(1_000_000 + 100 * 1024 * s, Ordering::Relaxed);
        f.evaluate(t0 + Duration::from_secs(s));
    }
    f.pause(t0 + Duration::from_secs(5));
    let v = f.evaluate(t0 + Duration::from_secs(35));
    f.resume(t0 + Duration::from_secs(35));
    assert_eq!(v, FloorVerdict::Ok);
}
