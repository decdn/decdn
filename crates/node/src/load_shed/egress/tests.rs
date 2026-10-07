use super::*;

#[test]
fn first_sample_is_instant_rate() {
    let m = EgressMeter::new();
    m.record(2_000);
    // 2000 bytes over a 1s interval, EWMA seeded from the first instant rate.
    assert_eq!(m.sample(1), 2_000);
    assert_eq!(m.current_bps(), 2_000);
}

#[test]
fn ewma_halves_toward_new_rate() {
    let m = EgressMeter::new();
    m.record(2_000);
    assert_eq!(m.sample(1), 2_000);
    // Next interval delivers 0: instant rate 0, EWMA = (2000 + 0)/2 = 1000.
    assert_eq!(m.sample(1), 1_000);
}

#[test]
fn rate_uses_bytes_since_last_sample_over_interval() {
    let m = EgressMeter::new();
    m.record(8_000);
    // 8000 bytes over a 2s interval = 4000 B/s instant; first sample = instant.
    assert_eq!(m.sample(2), 4_000);
}

#[test]
fn zero_interval_is_ignored() {
    let m = EgressMeter::new();
    m.record(1_000);
    // A zero interval must not divide-by-zero; it returns the prior rate.
    assert_eq!(m.sample(0), 0);
}
