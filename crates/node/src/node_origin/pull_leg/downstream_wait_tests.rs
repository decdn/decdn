use std::sync::Arc;
use std::time::Duration;

use decdn_cache::{FillSession, Hash};

use super::DownstreamWait;
use crate::metrics::Metrics;
use decdn_client::{DownstreamFrontier, PacingWait, WaitReason};

/// A standalone session and a wait over its downstream frontiers.
fn session_and_hook() -> (Arc<FillSession>, DownstreamWait) {
    let session = FillSession::new(bao_tree::blake3::Hash::from([7; 32]), 1 << 20);
    let hook = DownstreamWait::for_session(&session, Hash::from([7; 32]), Arc::new(Metrics::new()));
    (session, hook)
}

/// Each pause reason bumps its own counter, so a minimum-draw pause does not
/// read as the window binding.
#[tokio::test]
async fn each_wait_reason_bumps_its_own_counter() {
    fn count(metrics: &Metrics, name: &str) -> u64 {
        let text = metrics.encode().unwrap();
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.trim().parse().ok())
            .unwrap_or(0)
    }
    const MIN_DRAW: &str = "decdn_node_pull_through_min_draw_waits_total ";
    const PAUSED: &str = "decdn_node_pull_through_window_paused_total ";
    let session = FillSession::new(bao_tree::blake3::Hash::from([7; 32]), 1 << 20);
    let metrics = Arc::new(Metrics::new());
    let hook = DownstreamWait::for_session(&session, Hash::from([7; 32]), Arc::clone(&metrics));
    // A demand that differs from the observed one returns the wait at once.
    let leg = session.demand_slot();
    leg.stand(64 * 1024);
    hook.wait(DownstreamFrontier::default(), WaitReason::MinDraw)
        .await;
    assert_eq!(count(&metrics, MIN_DRAW), 1);
    assert_eq!(count(&metrics, PAUSED), 0);
    hook.wait(DownstreamFrontier::default(), WaitReason::WindowFull)
        .await;
    assert_eq!(count(&metrics, PAUSED), 1);
    assert_eq!(
        count(&metrics, "decdn_node_pull_through_wait_seconds_count "),
        2,
        "each pause records its length once",
    );
}

/// A pause the leg cancels still records its length.
#[tokio::test(start_paused = true)]
async fn a_cancelled_pause_records_its_length() {
    let session = FillSession::new(bao_tree::blake3::Hash::from([7; 32]), 1 << 20);
    let metrics = Arc::new(Metrics::new());
    let hook = DownstreamWait::for_session(&session, Hash::from([7; 32]), Arc::clone(&metrics));
    let cancelled = tokio::time::timeout(
        Duration::from_secs(5),
        hook.wait(DownstreamFrontier::default(), WaitReason::WindowFull),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "nothing advances, so the pause runs until cancelled"
    );
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_node_pull_through_wait_seconds_count 1"),
        "the cancelled pause records once:\n{text}"
    );
}

/// A pause that outlives the warning threshold keeps waiting, and records
/// its whole length once it ends.
#[tokio::test(start_paused = true)]
async fn a_long_pause_keeps_waiting_and_records_its_length() {
    let session = FillSession::new(bao_tree::blake3::Hash::from([7; 32]), 1 << 20);
    let metrics = Arc::new(Metrics::new());
    let hook = DownstreamWait::for_session(&session, Hash::from([7; 32]), Arc::clone(&metrics));
    let pause = hook.wait(DownstreamFrontier::default(), WaitReason::WindowFull);
    tokio::pin!(pause);
    let early = tokio::time::timeout(super::PULL_WAIT_WARN_AFTER * 2, &mut pause).await;
    assert!(early.is_err(), "the warning must not end the pause");
    let leg = session.demand_slot();
    leg.stand(64 * 1024);
    pause.await;
    let text = metrics.encode().unwrap();
    let sum: f64 = text
        .lines()
        .find_map(|l| {
            l.strip_prefix("decdn_node_pull_through_wait_seconds_sum ")?
                .trim()
                .parse()
                .ok()
        })
        .unwrap();
    assert!(
        sum >= (super::PULL_WAIT_WARN_AFTER * 2).as_secs_f64(),
        "the recorded length spans the whole pause, got {sum}",
    );
}

/// A pull whose window is full draws one floor for the serve leg parked at its
/// frontier, even while a serve leg of a sibling fill of the same blob waits
/// further down. Pacing to `Wait` here leaves both legs waiting on each other.
#[tokio::test]
async fn a_full_window_draws_for_its_parked_leg_despite_a_far_sibling() {
    use decdn_client::{PULL_WINDOW_FLOOR, PaceDecision, PaceState, Pacer, WindowPacer};

    const G: u64 = 16 * 1024;
    const WINDOW: u64 = 64 * G;
    let total = 1024 * G;
    let root = bao_tree::blake3::Hash::from([9; 32]);
    let hash = Hash::from([9; 32]);
    let registry = Arc::new(decdn_cache::FillRegistry::new());
    let fill = |start: u64, len: u64| {
        let session = FillSession::starting_at(root, total, start);
        session.set_covered(
            decdn_cache::range_pull::align_range(start, len, total)
                .unwrap()
                .chunk_ranges()
                .clone(),
        );
        let lease = registry.register_fill(hash, &session);
        (session, lease)
    };
    let (near, _near_lease) = fill(0, 512 * G);
    let (far, _far_lease) = fill(512 * G, 512 * G);
    let hook = DownstreamWait::for_session(&near, hash, Arc::new(Metrics::new()));

    // The near pull has run a full window ahead of what its client paid, and its
    // serve leg is parked on the next group; the far fill's leg waits further on.
    let pulled = 100 * G;
    let far_leg = far.demand_slot();
    far_leg.stand(700 * G);
    let near_leg = near.demand_slot();
    near_leg.stand(pulled + G);

    let state = PaceState {
        cleared_bytes: pulled,
        requested_bytes: 512 * G,
        remaining_deposit: alloy::primitives::U256::from(1_000_000u64),
        next_voucher_cost: alloy::primitives::U256::from(1u64),
        pulled_frontier: pulled,
        gap_remaining: 512 * G - pulled,
        downstream: DownstreamFrontier {
            served_paid: pulled - WINDOW,
            ..hook.frontier()
        },
    };
    assert_eq!(
        WindowPacer::new(WINDOW).decide(&state),
        PaceDecision::Draw {
            up_to_bytes: PULL_WINDOW_FLOOR
        },
        "the parked leg's floor must not be hidden by the far sibling"
    );
}

/// The #1673 race on the serve demand: a serve encoder parks and stands its
/// demand between the pacer's `Wait` decision and the pull parking, with no
/// served-paid advance at all. `wait` must see the demand move and return at
/// once, or the pull waits for a payment the parked encoder blocks (#1893).
#[tokio::test]
async fn a_racing_demand_advance_before_the_park_is_not_lost() {
    let (session, hook) = session_and_hook();

    let leg = session.demand_slot();
    leg.stand(64 * 1024);

    tokio::time::timeout(
        Duration::from_secs(5),
        hook.wait(DownstreamFrontier::default(), WaitReason::WindowFull),
    )
    .await
    .expect("wait must observe the raced demand advance, not wedge on a lost notify");
}

/// A pull already parked on its window wakes whenever the nearest serve demand
/// changes, with no payment at all: a leg parking nearer, or the nearest leg
/// moving on. A leg parking further out leaves the nearest demand, and the
/// pull, as they were. Pins that a [`decdn_cache::DemandSlot`] notifies the same
/// wakeup `DownstreamWait::for_session` arms (#1893).
#[tokio::test]
async fn a_nearest_demand_change_wakes_a_parked_pull_and_a_farther_one_does_not() {
    async fn parks(hook: &DownstreamWait) -> bool {
        tokio::time::timeout(
            Duration::from_millis(50),
            hook.wait(hook.frontier(), WaitReason::WindowFull),
        )
        .await
        .is_err()
    }
    async fn wakes(hook: &DownstreamWait, observed: DownstreamFrontier, change: impl FnOnce()) {
        let wait = hook.wait(observed, WaitReason::WindowFull);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), wait.as_mut())
                .await
                .is_err(),
            "with no change the wait stays parked"
        );
        change();
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("a nearest-demand change must wake the parked pull");
    }

    let (session, hook) = session_and_hook();
    let near = session.demand_slot();
    near.stand(64 * 1024);
    assert!(parks(&hook).await, "with no change the wait stays parked");

    let far = session.demand_slot();
    far.stand(80 * 1024);
    assert!(
        parks(&hook).await,
        "a farther demand leaves the nearest one, and the pull, as they were"
    );

    let nearer = session.demand_slot();
    wakes(&hook, hook.frontier(), || nearer.stand(32 * 1024)).await;
    wakes(&hook, hook.frontier(), || nearer.withdraw()).await;
    wakes(&hook, hook.frontier(), || drop(near)).await;
    assert_eq!(hook.frontier().serve_demand, 80 * 1024);
}

/// The #1673 race: the serve leg advances the frontier and fires its wakeup in
/// the gap between the pacer reading `observed` and the pull parking. The
/// notify wakes nobody (no waiter registered, no permit stored). `wait` must
/// re-read the frontier after arming and return at once; an edge-triggered wait
/// wedges here forever.
#[tokio::test]
async fn a_racing_advance_before_the_park_is_not_lost() {
    let (session, hook) = session_and_hook();

    // The advance + notify land BEFORE `wait` is polled — the lost-wakeup window.
    session.advance_served(64 * 1024);

    tokio::time::timeout(
        Duration::from_secs(5),
        hook.wait(DownstreamFrontier::default(), WaitReason::WindowFull),
    )
    .await
    .expect("wait must observe the raced advance, not wedge on a lost notify");
}

/// The ordinary path still parks and wakes: with no advance yet, `wait` blocks,
/// then resolves on a later advance from the serve leg.
#[tokio::test]
async fn a_later_advance_wakes_the_parked_wait() {
    let (session, hook) = session_and_hook();

    let advance = async {
        // Let `wait` arm + park first, then advance.
        tokio::task::yield_now().await;
        session.advance_served(64 * 1024);
    };
    tokio::join!(
        async {
            tokio::time::timeout(
                Duration::from_secs(5),
                hook.wait(DownstreamFrontier::default(), WaitReason::WindowFull),
            )
            .await
            .expect("a later advance must wake the parked wait");
        },
        advance,
    );
}
