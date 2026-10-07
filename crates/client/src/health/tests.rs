use super::{COOL_BASE, COOL_CAP, Health, PeerHealth};
use crate::fault::Fault;
use alloy::primitives::{Address, U256};
use tokio::time::{Duration, Instant};

const A: Address = Address::repeat_byte(0xA1);

#[tokio::test(start_paused = true)]
async fn a_source_fault_cools_and_the_cooldown_doubles_to_the_cap() {
    let health = PeerHealth::default();
    let now = Instant::now();
    health.record(A, Fault::Source, now, U256::ZERO);
    assert_eq!(health.cooling_until(A, now), Some(now + COOL_BASE));
    assert!(!health.usable(A, now, U256::ZERO));
    assert!(health.usable(A, now + COOL_BASE, U256::ZERO));

    let later = now + COOL_BASE;
    health.record(A, Fault::Source, later, U256::ZERO);
    assert_eq!(health.cooling_until(A, later), Some(later + COOL_BASE * 2));

    let mut t = later;
    for _ in 0..10 {
        t += COOL_CAP;
        health.record(A, Fault::Source, t, U256::ZERO);
    }
    assert_eq!(health.cooling_until(A, t), Some(t + COOL_CAP));
}

#[tokio::test(start_paused = true)]
async fn progress_resets_the_streak() {
    let health = PeerHealth::default();
    let now = Instant::now();
    health.record(A, Fault::Source, now, U256::ZERO);
    health.record(A, Fault::Source, now + COOL_BASE, U256::ZERO);
    health.record_progress(A);
    assert_eq!(health.health(A), Health::Healthy { streak: 0 });
    let t = now + Duration::from_secs(10);
    health.record(A, Fault::Source, t, U256::ZERO);
    assert_eq!(health.cooling_until(A, t), Some(t + COOL_BASE));
}

#[tokio::test(start_paused = true)]
async fn an_unaffordable_source_returns_when_the_deposit_rises() {
    let health = PeerHealth::default();
    let now = Instant::now();
    let deposit = U256::from(100u64);
    health.record(A, Fault::Unaffordable, now, deposit);
    assert!(!health.usable(A, now + COOL_CAP * 10, deposit));
    assert!(health.usable(A, now, deposit + U256::from(1u64)));
}

#[tokio::test(start_paused = true)]
async fn transient_and_fatal_faults_leave_the_source_alone() {
    let health = PeerHealth::default();
    let now = Instant::now();
    health.record(A, Fault::Transient, now, U256::ZERO);
    health.record(
        A,
        Fault::Fatal(crate::fault::FatalScope::Command),
        now,
        U256::ZERO,
    );
    assert_eq!(health.health(A), Health::Healthy { streak: 0 });
}
