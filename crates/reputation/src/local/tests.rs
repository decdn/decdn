use super::*;
use anyhow::{Context, ensure};
use iroh::SecretKey;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

/// Whole seconds in a week, for advancing [`ManualClock`] in the decay tests.
const WEEK_SECS: u64 = 7 * 24 * 3600;

/// A test clock whose "now" is set explicitly, so decay is exercised by
/// advancing simulated weeks rather than sleeping.
#[derive(Debug)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(secs: u64) -> Self {
        Self(AtomicU64::new(secs))
    }
    fn set(&self, secs: u64) {
        self.0.store(secs, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_secs(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Build a store on a [`ManualClock`] starting at t=0, returning both so the
/// test can advance time.
fn with_manual_clock(
    config: LocalReputationConfig,
) -> anyhow::Result<(LocalReputation, Arc<ManualClock>)> {
    let clock = Arc::new(ManualClock::new(0));
    let r = LocalReputation::with_clock(config, Arc::clone(&clock) as Arc<dyn Clock>)?;
    Ok((r, clock))
}

fn fresh_peer() -> NodeId {
    SecretKey::generate().public()
}

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

/// Looser comparison for closed-form decay values (`powf` rounding).
fn approx_eps(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() < eps
}

/// A config that writes an interaction's sample straight through (alpha=1,
/// no clamp), so a single record seeds an exact score to decay from.
fn seeding_config() -> LocalReputationConfig {
    LocalReputationConfig {
        alpha: 1.0,
        max_delta_per_update: 1.0,
        ..LocalReputationConfig::default()
    }
}

fn delivered_full() -> Outcome {
    Outcome::Delivered {
        bytes: 1024 * 1024 * 1024, // reference throughput in 1s → speed 1.0
        elapsed: Duration::from_secs(1),
    }
}

#[test]
fn default_config_matches_adr_008_section_3() -> anyhow::Result<()> {
    let c = LocalReputationConfig::default();
    ensure!(approx(c.alpha, 0.1), "alpha drift: {}", c.alpha);
    ensure!(
        approx(c.initial_score, 0.5),
        "initial drift: {}",
        c.initial_score
    );
    ensure!(approx(c.speed_weight, 0.4));
    ensure!(approx(c.correctness_weight, 0.4));
    ensure!(approx(c.reachability_weight, 0.2));
    ensure!(c.reference_bps == 1024 * 1024 * 1024);
    // §14a defers clamping; default is no-op (1.0).
    ensure!(approx(c.max_delta_per_update, 1.0));
    // §Score Decay: half-life default is 3 days.
    ensure!(
        c.decay_half_life_secs == 3 * 24 * 3600,
        "half-life drift: {}",
        c.decay_half_life_secs
    );
    let sum = c.speed_weight + c.correctness_weight + c.reachability_weight;
    ensure!((sum - 1.0).abs() < 1e-9, "weight sum drift: {sum}");
    Ok(())
}

#[test]
fn invalid_config_rejected() {
    let bad = LocalReputationConfig {
        alpha: f64::NAN,
        ..LocalReputationConfig::default()
    };
    assert!(matches!(
        LocalReputation::new(bad),
        Err(ConfigError::OutOfUnitInterval { field: "alpha", .. })
    ));

    let bad = LocalReputationConfig {
        reference_bps: 0,
        ..LocalReputationConfig::default()
    };
    assert!(matches!(
        LocalReputation::new(bad),
        Err(ConfigError::ZeroReferenceBps)
    ));

    let bad = LocalReputationConfig {
        speed_weight: 0.5, // 0.5 + 0.4 + 0.2 = 1.1
        ..LocalReputationConfig::default()
    };
    assert!(matches!(
        LocalReputation::new(bad),
        Err(ConfigError::WeightsDoNotSumToOne { .. })
    ));

    let bad = LocalReputationConfig {
        initial_score: 1.5,
        ..LocalReputationConfig::default()
    };
    assert!(matches!(
        LocalReputation::new(bad),
        Err(ConfigError::OutOfUnitInterval {
            field: "initial_score",
            ..
        })
    ));
}

#[test]
fn score_for_unseen_peer_returns_initial() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    ensure!(approx(r.score(fresh_peer()), 0.5));
    Ok(())
}

#[test]
fn delivered_at_reference_speed_pulls_score_up() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p = fresh_peer();
    // interaction = 0.4*1 + 0.4 + 0.2 = 1.0
    // EWMA from 0.5: 0.9*0.5 + 0.1*1.0 = 0.55 (no clamp; default max_delta=1.0)
    let next = r.record(
        p,
        Outcome::Delivered {
            bytes: 1024 * 1024 * 1024,
            elapsed: Duration::from_secs(1),
        },
    );
    ensure!(approx(next, 0.55), "got {next}");
    Ok(())
}

#[test]
fn delivered_at_zero_speed_isolates_correctness_weight() -> anyhow::Result<()> {
    // bytes=0 → speed_score=0; this lets us pin the *correctness* weight
    // (0.4) independently of speed: interaction = 0 + 0.4 + 0.2 = 0.6.
    // EWMA from 0.5: 0.51.
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p = fresh_peer();
    let next = r.record(
        p,
        Outcome::Delivered {
            bytes: 0,
            elapsed: Duration::from_secs(1),
        },
    );
    ensure!(approx(next, 0.51), "got {next}");
    Ok(())
}

#[test]
fn unreachable_pulls_score_down() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p = fresh_peer();
    // interaction = 0; EWMA from 0.5: 0.45
    let next = r.record(p, Outcome::Unreachable);
    ensure!(approx(next, 0.45), "got {next}");
    Ok(())
}

#[test]
fn corruption_holds_score_near_reachability_weight() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p = fresh_peer();
    // Corruption interaction = 0.2; long-run fixed point of EWMA = 0.2.
    for _ in 0..200 {
        r.record(p, Outcome::Corruption);
    }
    let s = r.score(p);
    ensure!((s - 0.2).abs() < 1e-3, "score = {s}");
    Ok(())
}

#[test]
fn clamp_caps_per_update_movement() -> anyhow::Result<()> {
    // Opt into the §8 ±0.05 per-report clamp; alpha=1 makes the
    // candidate next value equal the sample so the clamp is the only
    // invariant under test. Without it, prev=0.5 + sample=0 would land
    // at 0; with the 0.05 cap it lands at 0.45.
    let cfg = LocalReputationConfig {
        alpha: 1.0,
        max_delta_per_update: 0.05,
        ..LocalReputationConfig::default()
    };
    let r = LocalReputation::new(cfg)?;
    let p = fresh_peer();
    let next = r.record(p, Outcome::Unreachable);
    ensure!(approx(next, 0.45), "got {next}");
    Ok(())
}

#[test]
fn builder_opts_into_adr008_per_report_clamp() -> anyhow::Result<()> {
    ensure!(approx(LOCAL_SCORE_MAX_DELTA_PER_REPORT, 0.05));
    // Same invariant as `clamp_caps_per_update_movement`, but reached through
    // the public builder the node wiring uses (the struct is
    // `#[non_exhaustive]`, so a downstream crate cannot set the field with a
    // struct-update literal).
    let cfg = LocalReputationConfig::default()
        .with_max_delta_per_update(LOCAL_SCORE_MAX_DELTA_PER_REPORT);
    ensure!(
        approx(cfg.max_delta_per_update, 0.05),
        "builder did not set cap"
    );
    let r = LocalReputation::new(cfg)?;
    let p = fresh_peer();
    // Drive the score up so a single Unreachable would swing by more than
    // 0.05 (unclamped delta = 0.1 * prev) — the clamp must bind.
    for _ in 0..20 {
        r.record(
            p,
            Outcome::Delivered {
                bytes: 10 * 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
    }
    let before = r.score(p);
    ensure!(
        before > 0.5,
        "precondition: score should have climbed, got {before}"
    );
    let after = r.record(p, Outcome::Unreachable);
    ensure!(
        approx(after, before - 0.05),
        "clamp did not bind: before={before} after={after}"
    );
    Ok(())
}

#[test]
fn region_latency_mismatch_scores_like_unreachable() -> anyhow::Result<()> {
    // ADR 030 penalty is a fully negative sample — assert the equivalence the
    // name promises (shared `=> 0.0` arm) directly, so the test survives any
    // change to the default alpha/initial_score rather than pinning 0.45.
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let via_penalty = r.record(fresh_peer(), Outcome::RegionLatencyMismatch);
    let via_unreachable = r.record(fresh_peer(), Outcome::Unreachable);
    ensure!(
        approx(via_penalty, via_unreachable),
        "penalty {via_penalty} != unreachable {via_unreachable}"
    );
    Ok(())
}

#[test]
fn speed_score_function_is_correct() {
    let bps = 1024 * 1024 * 1024; // 1 GiB/s reference
    let one_sec = Duration::from_secs(1);
    assert!(approx(speed_score(bps, one_sec, bps), 1.0));
    assert!(approx(speed_score(2 * bps, one_sec, bps), 1.0));
    assert!(speed_score(bps / 10, one_sec, bps) < speed_score(bps, one_sec, bps));
    assert!(approx(speed_score(0, one_sec, bps), 0.0));
    assert!(approx(speed_score(1, Duration::ZERO, bps), 0.0));
    assert!(approx(speed_score(1, one_sec, 0), 0.0));
}

#[test]
fn reference_bps_override_changes_speed_baseline() -> anyhow::Result<()> {
    let cfg = LocalReputationConfig {
        reference_bps: 1024 * 1024, // 1 MiB/s baseline
        ..LocalReputationConfig::default()
    };
    let r = LocalReputation::new(cfg)?;
    let p = fresh_peer();
    // 1 MiB in 1s now equals baseline → interaction = 1.0; EWMA → 0.55
    let next = r.record(
        p,
        Outcome::Delivered {
            bytes: 1024 * 1024,
            elapsed: Duration::from_secs(1),
        },
    );
    ensure!(approx(next, 0.55), "got {next}");
    Ok(())
}

#[test]
fn slow_delivery_scores_between_corruption_and_full() -> anyhow::Result<()> {
    let cfg = LocalReputationConfig::default();
    let r_corrupt = LocalReputation::new(cfg.clone())?;
    let r_slow = LocalReputation::new(cfg.clone())?;
    let r_full = LocalReputation::new(cfg)?;
    let p = fresh_peer();
    let corrupt = r_corrupt.record(p, Outcome::Corruption);
    // 1 MiB/s vs. the 1 GiB/s reference: log curve still scores this well
    // above zero but below a full-reference-speed delivery.
    let slow = r_slow.record(
        p,
        Outcome::Delivered {
            bytes: 1024 * 1024,
            elapsed: Duration::from_secs(1),
        },
    );
    let full = r_full.record(
        p,
        Outcome::Delivered {
            bytes: 10 * 1024 * 1024,
            elapsed: Duration::from_secs(1),
        },
    );
    ensure!(corrupt < slow, "corrupt={corrupt} slow={slow}");
    ensure!(slow < full, "slow={slow} full={full}");
    Ok(())
}

#[test]
fn independent_peers_tracked_separately() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p_good = fresh_peer();
    let p_bad = fresh_peer();
    for _ in 0..50 {
        r.record(
            p_good,
            Outcome::Delivered {
                bytes: 1024 * 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
        r.record(p_bad, Outcome::Unreachable);
    }
    let good = r.score(p_good);
    let bad = r.score(p_bad);
    ensure!(good > 0.9, "good={good}");
    ensure!(bad < 0.05, "bad={bad}");
    Ok(())
}

#[test]
fn zero_elapsed_does_not_panic_and_yields_zero_speed() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p = fresh_peer();
    // speed_score=0 → interaction = 0 + 0.4 + 0.2 = 0.6; EWMA → 0.51
    let next = r.record(
        p,
        Outcome::Delivered {
            bytes: 1,
            elapsed: Duration::ZERO,
        },
    );
    ensure!(approx(next, 0.51), "got {next}");
    Ok(())
}

#[test]
fn record_returns_post_update_score() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p = fresh_peer();
    let returned = r.record(p, Outcome::Unreachable);
    let queried = r.score(p);
    ensure!(approx(returned, queried));
    // Anchor the value so a buggy `record` returning `prev` (0.5) instead
    // of `next` (0.45) cannot pass via the queried==returned tautology.
    ensure!(approx(returned, 0.45), "expected 0.45, got {returned}");
    Ok(())
}

#[test]
fn snapshot_includes_all_observed_peers() -> anyhow::Result<()> {
    let r = LocalReputation::new(LocalReputationConfig::default())?;
    let p1 = fresh_peer();
    let p2 = fresh_peer();
    r.record(p1, Outcome::Unreachable); // 0.45
    r.record(p2, Outcome::Corruption); // 0.9*0.5 + 0.1*0.2 = 0.47
    let snap: HashMap<NodeId, f64> = r.snapshot().into_iter().collect();
    ensure!(snap.len() == 2, "expected 2 entries, got {}", snap.len());
    let s1 = *snap.get(&p1).context("p1 missing from snapshot")?;
    let s2 = *snap.get(&p2).context("p2 missing from snapshot")?;
    ensure!(approx(s1, 0.45), "p1 score={s1}");
    ensure!(approx(s2, 0.47), "p2 score={s2}");
    Ok(())
}

#[test]
fn concurrent_record_and_score_does_not_lose_updates() -> anyhow::Result<()> {
    let r = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let p = fresh_peer();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let r = Arc::clone(&r);
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                r.record(p, Outcome::Unreachable);
                let _ = r.score(p);
            }
        }));
    }
    for h in handles {
        h.join()
            .map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
    }
    let snap = r.snapshot();
    ensure!(
        snap.len() == 1,
        "expected exactly one peer entry, got {}",
        snap.len()
    );
    let s = r.score(p);
    ensure!((0.0..=1.0).contains(&s), "score out of range: {s}");
    // After 8000 Unreachable events from 0.5, EWMA decays as 0.5 * 0.9^n
    // — well below 1e-100, so any non-trivial value would indicate a
    // lost update from a torn read-modify-write.
    ensure!(s < 1e-4, "expected near-zero score, got {s}");
    Ok(())
}

#[test]
fn score_decays_by_half_each_half_life() -> anyhow::Result<()> {
    let (r, clock) = with_manual_clock(seeding_config())?;
    let p = fresh_peer();
    ensure!(approx(r.record(p, delivered_full()), 1.0), "seed != 1.0");
    let hl = LocalReputationConfig::default().decay_half_life_secs;
    for (n, expected) in [(1u64, 0.75), (2, 0.625), (3, 0.5625)] {
        clock.set(n * hl);
        let got = r.score(p);
        ensure!(
            approx_eps(got, expected, 1e-9),
            "n={n}: got {got}, want {expected}"
        );
    }
    Ok(())
}

#[test]
fn low_score_rehabilitates_by_half_each_half_life() -> anyhow::Result<()> {
    let (r, clock) = with_manual_clock(seeding_config())?;
    let p = fresh_peer();
    ensure!(
        approx(r.record(p, Outcome::Unreachable), 0.0),
        "seed != 0.0"
    );
    let hl = LocalReputationConfig::default().decay_half_life_secs;
    for (n, expected) in [(1u64, 0.25), (2, 0.375)] {
        clock.set(n * hl);
        let got = r.score(p);
        ensure!(
            approx_eps(got, expected, 1e-9),
            "n={n}: got {got}, want {expected}"
        );
    }
    Ok(())
}

#[test]
fn record_decays_stored_score_before_folding() -> anyhow::Result<()> {
    // With alpha=0.1, one full delivery at t=0 lands the peer at 0.55. After
    // one half-life the *stored* 0.55 has decayed to 0.5 + 0.05·0.5 =
    // 0.525; the next Unreachable must fold from that decayed value
    // (0.9·0.525 = 0.4725), not from the stale 0.55 (→ 0.495).
    let (r, clock) = with_manual_clock(LocalReputationConfig::default())?;
    let p = fresh_peer();
    ensure!(approx(r.record(p, delivered_full()), 0.55), "seed != 0.55");
    clock.set(LocalReputationConfig::default().decay_half_life_secs);
    let next = r.record(p, Outcome::Unreachable);
    ensure!(
        approx_eps(next, 0.4725, 1e-9),
        "expected fold from decayed base, got {next}"
    );
    Ok(())
}

#[test]
fn decay_disabled_when_half_life_zero() -> anyhow::Result<()> {
    let cfg = LocalReputationConfig {
        decay_half_life_secs: 0,
        ..seeding_config()
    };
    let (r, clock) = with_manual_clock(cfg)?;
    let p = fresh_peer();
    ensure!(approx(r.record(p, delivered_full()), 1.0), "seed != 1.0");
    clock.set(100 * WEEK_SECS);
    ensure!(
        approx(r.score(p), 1.0),
        "half-life 0 should freeze the score"
    );
    Ok(())
}

#[test]
fn backward_clock_neither_decays_nor_rewinds_the_timestamp() -> anyhow::Result<()> {
    // Seed at week 10, then rewind the clock to week 5. A read at the earlier
    // time must not "decay" (elapsed saturates at 0), and a record at the
    // earlier time must not rewind `last_update_secs` — otherwise a later
    // forward read would over-decay.
    let (r, clock) = with_manual_clock(seeding_config())?;
    let p = fresh_peer();
    let hl = LocalReputationConfig::default().decay_half_life_secs;
    clock.set(10 * hl);
    ensure!(approx(r.record(p, delivered_full()), 1.0), "seed != 1.0");

    clock.set(5 * hl); // clock goes backwards
    ensure!(
        approx(r.score(p), 1.0),
        "backward read decayed: {}",
        r.score(p)
    );
    // A record while the clock is behind keeps the stored timestamp at 10*hl.
    ensure!(
        approx(r.record(p, delivered_full()), 1.0),
        "backward record moved score"
    );

    // Forward to 12*hl: only 2 half-lives of decay from the pinned 10*hl
    // timestamp, i.e. 0.5 + 0.5·0.5^2 = 0.625 — not decay measured from 5*hl.
    clock.set(12 * hl);
    let got = r.score(p);
    ensure!(
        approx_eps(got, 0.625, 1e-9),
        "timestamp rewound: got {got}, want 0.625"
    );
    Ok(())
}

#[test]
fn evict_drops_only_idle_neutralised_entries() -> anyhow::Result<()> {
    // Eviction requires BOTH: decayed within the neutral band AND idle
    // longer than EVICT_IDLE_WEEKS (26). The three peers each break exactly
    // one leg of that AND, or satisfy both.
    let (r, clock) = with_manual_clock(seeding_config())?;
    let evictable = fresh_peer(); // near-neutral AND idle > 26 wk → dropped
    let recent = fresh_peer(); // non-neutral, idle 0 → kept
    let not_idle_enough = fresh_peer(); // near-neutral but idle < 26 wk → kept

    // t=0: seed the peer that will age fully into neutrality by week 40.
    r.record(evictable, Outcome::Unreachable);
    // Week 30: seed the peer that, at week 40, is idle only 10 weeks — near
    // neutral (half-life is days) but below the 26-week idle floor.
    clock.set(30 * WEEK_SECS);
    r.record(not_idle_enough, Outcome::Unreachable);
    // Week 40: refresh `recent` (idle 0, score 0.0), then evict.
    clock.set(40 * WEEK_SECS);
    r.record(recent, Outcome::Unreachable);

    // Preconditions the eviction predicate keys on: two are inside the band,
    // one is not; idle ages are 40 wk / 10 wk / 0 wk respectively.
    ensure!(
        (r.score(evictable) - 0.5).abs() <= EVICT_NEUTRAL_BAND,
        "evictable not within band: {}",
        r.score(evictable)
    );
    ensure!(
        (r.score(not_idle_enough) - 0.5).abs() <= EVICT_NEUTRAL_BAND,
        "not_idle_enough not within band: {}",
        r.score(not_idle_enough)
    );
    ensure!(
        (r.score(recent) - 0.5).abs() > EVICT_NEUTRAL_BAND,
        "recent unexpectedly near neutral: {}",
        r.score(recent)
    );

    r.evict();

    let present: HashMap<NodeId, f64> = r.snapshot().into_iter().collect();
    ensure!(
        !present.contains_key(&evictable),
        "idle+neutral entry should have been evicted"
    );
    ensure!(
        present.contains_key(&recent),
        "recently-updated (non-neutral) entry should be retained"
    );
    ensure!(
        present.contains_key(&not_idle_enough),
        "near-neutral but not-idle-enough entry should be retained"
    );
    Ok(())
}

#[test]
fn snapshot_reflects_decay() -> anyhow::Result<()> {
    let (r, clock) = with_manual_clock(seeding_config())?;
    let p = fresh_peer();
    r.record(p, delivered_full()); // 1.0 at t=0
    let hl = LocalReputationConfig::default().decay_half_life_secs;
    clock.set(2 * hl);
    let snap: HashMap<NodeId, f64> = r.snapshot().into_iter().collect();
    let s = *snap.get(&p).context("peer missing from snapshot")?;
    ensure!(
        approx_eps(s, 0.625, 1e-9),
        "snapshot did not decay: got {s}"
    );
    Ok(())
}
