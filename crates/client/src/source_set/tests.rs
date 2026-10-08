use super::{DISCOVERY_BASE, Holder, NoAffordableSource, SourceProvider, SourceSet};
use crate::fault::{Fault, LaneBuildFault};
use crate::health::{COOL_BASE, PeerHealth};
use crate::source::{ScriptedSource, SourceFuture, ctx_with};
use crate::streamer::StreamCandidate;
use crate::{Cumulative, LaneLease, PoolLedger};
use alloy::primitives::{Address, U256};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant};

const A: Address = Address::repeat_byte(0xA1);
const B: Address = Address::repeat_byte(0xB2);

fn holder(provider: Address, rtt_ms: f64) -> Holder {
    Holder {
        provider,
        coverage: None,
        rtt_ms,
        probed_holder: true,
    }
}

/// A provider the probe did not report as a holder: a pull-through target.
fn non_holder(provider: Address, rtt_ms: f64) -> Holder {
    Holder {
        probed_holder: false,
        ..holder(provider, rtt_ms)
    }
}

struct FakeProvider {
    discover: Mutex<Vec<anyhow::Result<Vec<Holder>>>>,
    faults: Mutex<Vec<Address>>,
}

impl SourceProvider for FakeProvider {
    type Source = ScriptedSource;
    fn discover(&self, _hash: [u8; 32]) -> SourceFuture<'_, Vec<Holder>> {
        let next = self
            .discover
            .lock()
            .ok()
            .and_then(|mut d| d.pop())
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(async move { next })
    }
    fn connect<'a>(
        &'a self,
        _holder: &'a Holder,
    ) -> SourceFuture<'a, StreamCandidate<ScriptedSource>> {
        Box::pin(async { anyhow::bail!("tests call lane_built directly") })
    }
    fn on_source_fault(&self, holder: &Holder) {
        if let Ok(mut f) = self.faults.lock() {
            f.push(holder.provider);
        }
    }
}

fn provider(discover: Vec<anyhow::Result<Vec<Holder>>>) -> FakeProvider {
    FakeProvider {
        discover: Mutex::new(discover),
        faults: Mutex::new(Vec::new()),
    }
}

fn candidate() -> anyhow::Result<StreamCandidate<ScriptedSource>> {
    let src = ScriptedSource::new(vec![7u8; 1024])?;
    Ok(StreamCandidate {
        source: src,
        ctx: Arc::new(Mutex::new(ctx_with(0xA1, U256::ZERO))),
        ledger: Arc::new(PoolLedger::new(Cumulative::default())),
        coverage: None,
        lease: LaneLease::new(()),
        widen: None,
    })
}

/// A pushed holder joins the set and leaves the discovery backoff alone.
#[test]
fn holders_arrived_adds_a_holder_without_touching_discovery() {
    let provider = provider(Vec::new());
    let mut set = SourceSet::new(&provider, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    set.discovery_done(Ok(Vec::new()), Instant::now(), U256::ZERO);
    let backoff = set.discovery.map(|b| b.next_at);
    let looked = set.looked_at;
    let discovered = set.discovered_at;

    set.holders_arrived(vec![holder(B, 20.0)]);

    assert_eq!(set.holders().len(), 2);
    assert!(set.holder(B).is_some());
    assert_eq!(set.discovery.map(|b| b.next_at), backoff);
    assert_eq!(set.looked_at, looked);
    assert_eq!(set.discovered_at, discovered);
}

/// A pushed holder the set already knows updates in place.
#[test]
fn holders_arrived_updates_a_known_holder_in_place() {
    let provider = provider(Vec::new());
    let mut set = SourceSet::new(&provider, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    set.holders_arrived(vec![holder(A, 4.0)]);
    assert_eq!(set.holders().len(), 1);
    assert_eq!(set.holder(A).map(|h| h.rtt_ms), Some(4.0));
}

/// Two lanes on one provider would run two concurrent voucher streams on
/// one `(signer, provider)` watermark, so the static set refuses them.
#[test]
fn two_candidates_for_one_provider_are_refused() -> anyhow::Result<()> {
    let result = super::StaticSources::new(vec![candidate()?, candidate()?]);
    assert!(result.is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_nearest_usable_source_starts_first() {
    let p = provider(vec![]);
    let set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![holder(B, 30.0), holder(A, 10.0)],
    );
    let now = Instant::now();
    let first = set.next_to_start(now, U256::ZERO, &HashSet::new());
    assert_eq!(first.map(|h| h.provider), Some(A));
    let running = HashSet::from([A]);
    let second = set.next_to_start(now, U256::ZERO, &running);
    assert_eq!(second.map(|h| h.provider), Some(B));
}

#[tokio::test(start_paused = true)]
async fn a_source_fault_cools_the_source_and_reports_it() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let now = Instant::now();
    let fault = set.record_fault(A, &anyhow::anyhow!("reset"), None, now, U256::ZERO);
    assert_eq!(fault, Fault::Source);
    assert!(
        set.next_to_start(now, U256::ZERO, &HashSet::new())
            .is_none()
    );
    assert_eq!(set.next_wake(now), Some(now + COOL_BASE));
    assert!(
        set.next_to_start(now + COOL_BASE, U256::ZERO, &HashSet::new())
            .is_some()
    );
    assert_eq!(
        p.faults.lock().map(|f| f.clone()).unwrap_or_default(),
        vec![A]
    );
}

#[tokio::test(start_paused = true)]
async fn a_lane_build_error_retries_later_without_cooling_the_source() -> anyhow::Result<()> {
    let p = provider(vec![]);
    let health: Arc<PeerHealth> = Arc::default();
    let mut set = SourceSet::new(&p, [0; 32], Arc::clone(&health), vec![holder(A, 10.0)]);
    let now = Instant::now();
    let err = set
        .lane_built(A, Err(anyhow::anyhow!("rpc timed out")), now)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a failed build must be an error"))?;
    assert!(err.downcast_ref::<LaneBuildFault>().is_some());
    assert!(health.usable(A, now, U256::ZERO));
    assert!(
        set.next_to_start(now, U256::ZERO, &HashSet::new())
            .is_none()
    );
    assert_eq!(set.next_wake(now), Some(now + super::BUILD_RETRY_BASE));
    let later = now + super::BUILD_RETRY_BASE;
    assert!(
        set.next_to_start(later, U256::ZERO, &HashSet::new())
            .is_some()
    );
    let lane = set.lane_built(A, candidate(), later)?;
    assert!(Arc::ptr_eq(
        &lane,
        &set.cached_lane(A)
            .ok_or_else(|| anyhow::anyhow!("cached"))?
    ));
    Ok(())
}

/// A built lane that finds no free stream waits until the time its
/// caller names, and no longer however often it is refused: the wait
/// does not grow like a build's. Only the first refusal since the lane
/// last started reports itself, and a start clears the wait.
#[tokio::test(start_paused = true)]
async fn a_lane_refused_a_stream_waits_a_flat_retry() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let none_running = HashSet::new();
    let retry = Duration::from_secs(1);
    let mut now = Instant::now();
    for refusal in 0..4 {
        assert_eq!(set.start_refused(A, now + retry), refusal == 0);
        assert!(set.next_to_start(now, U256::ZERO, &none_running).is_none());
        assert_eq!(set.next_wake(now), Some(now + retry));
        now += retry;
        assert!(set.next_to_start(now, U256::ZERO, &none_running).is_some());
    }
    set.start_taken(A);
    assert_eq!(set.next_wake(now), None);
    assert!(
        set.start_refused(A, now + retry),
        "a refusal after a start is the first of a new run"
    );
}

#[tokio::test(start_paused = true)]
async fn a_discovery_error_backs_off_and_keeps_the_known_sources() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let now = Instant::now();
    set.record_fault(A, &anyhow::anyhow!("reset"), None, now, U256::ZERO);
    assert!(
        set.wants_discovery(now, U256::ZERO, 0, false),
        "starved: A is cooling"
    );
    set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
    assert!(set.holder(A).is_some());
    // `uncovered = true` keeps the want alive after A's 2 s cooldown ends,
    // so these two lines test the 5 s discovery backoff alone.
    assert!(!set.wants_discovery(now, U256::ZERO, 0, true));
    assert!(set.wants_discovery(now + DISCOVERY_BASE, U256::ZERO, 0, true));
}

#[tokio::test(start_paused = true)]
async fn discovery_merges_new_holders() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let now = Instant::now();
    set.discovery_done(Ok(vec![holder(A, 12.0), holder(B, 5.0)]), now, U256::ZERO);
    assert_eq!(
        set.next_to_start(now, U256::ZERO, &HashSet::new())
            .map(|h| h.provider),
        Some(B)
    );
    assert!(set.holder(A).is_some());
}

#[tokio::test(start_paused = true)]
async fn every_source_unaffordable_stops_only_after_a_fresh_discovery() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![holder(A, 10.0), holder(B, 20.0)],
    );
    let now = Instant::now();
    let dep = U256::from(100u64);
    let dry = || {
        anyhow::Error::new(crate::driver::PoolExhausted {
            gap_start: 0,
            gap_len: 1,
        })
    };
    set.record_fault(A, &dry(), None, now, dep);
    set.record_fault(B, &dry(), None, now, dep);
    assert!(set.exhausted(dep, true, false).is_none(), "top-ups left");
    assert!(
        set.exhausted(dep, false, false).is_none(),
        "no discovery at this deposit yet"
    );
    assert!(set.wants_discovery(now, dep, 0, false));
    set.discovery_done(Ok(vec![]), now, dep);
    let err = set.exhausted(dep, false, false);
    assert!(err.is_some_and(|e| e.downcast_ref::<NoAffordableSource>().is_some()));
    assert!(
        set.exhausted(dep + U256::from(1u64), false, false)
            .is_none(),
        "a top-up revives"
    );
}

fn not_found() -> anyhow::Error {
    anyhow::Error::new(crate::UpstreamRefused::mid_stream(
        decdn_protocol::client::StreamError::NotFound,
    ))
}

/// Record `times` `NotFound` answers from `provider`.
fn say_not_found<P: SourceProvider>(
    set: &mut SourceSet<'_, P>,
    provider: Address,
    times: u32,
    now: Instant,
) {
    for _ in 0..times {
        set.record_fault(provider, &not_found(), None, now, U256::ZERO);
    }
}

#[tokio::test(start_paused = true)]
async fn every_source_saying_not_found_ends_the_item() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![non_holder(A, 10.0), non_holder(B, 20.0)],
    );
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "B has not answered"
    );
    say_not_found(&mut set, B, super::ABSENT_AFTER_NOT_FOUND, now);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "no discovery since the last mark"
    );
    assert!(
        set.wants_discovery(now, U256::ZERO, 0, false),
        "unanimity skips the backoff"
    );
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    let err = set.exhausted(U256::ZERO, true, false);
    assert!(err.is_some_and(|e| e.downcast_ref::<super::NoSourceHasBlob>().is_some()));
}

/// A refusal the chain explains as a drained capability signer at the
/// refusing provider's rate.
fn drained(provider: Address, remaining: u64, rate_per_mb: u64) -> anyhow::Error {
    anyhow::Error::new(crate::SignerCapDrained {
        pool_id: alloy::primitives::B256::ZERO,
        signer: Address::ZERO,
        provider,
        remaining,
        rate_per_mb,
        expired: false,
    })
}

/// A probed holder that refuses the lane's signer as drained at its rate is
/// barred for the blob, with no peer-store failure stamp, while a cheaper
/// holder still starts. Once every holder is barred, the item ends with
/// the drained cause in the chain (#2338).
#[tokio::test(start_paused = true)]
async fn a_drained_signer_bars_each_refusing_holder_until_none_is_left() -> anyhow::Result<()> {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![holder(A, 10.0), holder(B, 20.0)],
    );
    let now = Instant::now();
    let fault = set.record_fault(A, &drained(A, 5, 10), None, now, U256::ZERO);
    assert_eq!(fault, Fault::Source);
    assert!(
        p.faults.lock().is_ok_and(|f| f.is_empty()),
        "no failure stamp"
    );
    let later = now + Duration::from_hours(1);
    assert_eq!(
        set.next_to_start(later, U256::ZERO, &HashSet::new())
            .map(|h| h.provider),
        Some(B),
        "the barred holder never starts again"
    );
    set.discovery_done(Ok(vec![holder(A, 10.0)]), now, U256::ZERO);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "B is left"
    );

    set.record_fault(B, &drained(B, 5, 20), None, now, U256::ZERO);
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    let err = set
        .exhausted(U256::ZERO, true, false)
        .ok_or_else(|| anyhow::anyhow!("every holder is barred"))?;
    assert!(err.downcast_ref::<super::NoSourceServesSigner>().is_some());
    assert_eq!(
        err.downcast_ref::<crate::SignerCapDrained>()
            .map(|d| d.provider),
        Some(B),
        "the last refusal is the cause"
    );
    assert_eq!(crate::classify(&err), Fault::Fatal(crate::FatalScope::Item));
    Ok(())
}

/// A probed holder's `NotFound` is a delivery fault only: it may mean load
/// shed or a per-signer cap, so it never marks the holder absent.
#[tokio::test(start_paused = true)]
async fn a_probed_holder_saying_not_found_is_never_absent() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, 10, now);
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    assert!(set.exhausted(U256::ZERO, true, false).is_none());
    let cooled = set.health().cooling_until(A, now).is_some_and(|until| {
        set.next_to_start(until, U256::ZERO, &HashSet::new())
            .is_some()
    });
    assert!(cooled, "the holder cools and comes back");
}

/// A probed holder of `blocks` of a four-block blob only.
fn partial(provider: Address, rtt_ms: f64, blocks: &[u32]) -> Holder {
    Holder {
        coverage: Some(decdn_protocol::Coverage::from_block_indices(
            4,
            blocks.iter().copied(),
        )),
        ..holder(provider, rtt_ms)
    }
}

/// Record `times` `NotFound` answers from `provider` for `range`.
fn say_not_found_in<P: SourceProvider>(
    set: &mut SourceSet<'_, P>,
    provider: Address,
    range: super::LaneRange,
    times: u32,
    now: Instant,
) {
    for _ in 0..times {
        set.record_fault(provider, &not_found(), Some(range), now, U256::ZERO);
    }
}

/// A range in discovery block 1, inside or outside the lane's coverage.
const fn block1(uncovered: bool) -> super::LaneRange {
    super::LaneRange {
        offset: 64 << 20,
        len: 64 << 20,
        landed: 0,
        past_end: false,
        uncovered,
    }
}

/// A range in discovery block 0, inside the lane's coverage.
const fn block0() -> super::LaneRange {
    super::LaneRange {
        offset: 0,
        len: 64 << 20,
        landed: 0,
        past_end: false,
        uncovered: false,
    }
}

/// A partial holder that says `NotFound` three times to ranges outside
/// its coverage is barred from pull-through. Refusals inside its coverage
/// never bar it, a verified byte inside its coverage keeps the bar, and a
/// verified byte outside it lifts the bar.
#[tokio::test(start_paused = true)]
async fn three_uncovered_refusals_bar_a_partial_holder_from_pull_through() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let now = Instant::now();
    say_not_found_in(&mut set, A, block0(), 10, now);
    assert!(!set.no_pull_through(A), "refusals inside its coverage");
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND - 1,
        now,
    );
    assert!(!set.no_pull_through(A), "two answers are not enough");
    say_not_found_in(&mut set, A, block1(true), 1, now);
    assert!(set.no_pull_through(A));
    set.record_progress(A);
    assert!(set.no_pull_through(A), "a covered byte keeps the bar");
    set.record_pull_through(A, now + Duration::from_millis(1));
    assert!(!set.no_pull_through(A), "an uncovered byte lifts the bar");
}

/// #2281: a partial holder that refuses a block its coverage claims, three
/// times, loses that block from its coverage and keeps the others. It is
/// not barred from pull-through, a probe that reports the stale claim
/// again does not bring the block back, and the drop ends with its time.
#[tokio::test(start_paused = true)]
async fn three_covered_refusals_drop_the_block_from_a_partial_holders_coverage() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![partial(A, 10.0, &[0, 1]), holder(B, 20.0)],
    );
    let now = Instant::now();
    let at = say_covered_not_found(
        &mut set,
        A,
        block1(false),
        super::ABSENT_AFTER_NOT_FOUND - 1,
        now,
    );
    assert!(covers(&set, A, 1), "two answers are not enough");
    let at = say_covered_not_found(&mut set, A, block1(false), 1, at + crate::health::COOL_BASE);
    assert!(!covers(&set, A, 1), "the refused block leaves the coverage");
    assert!(covers(&set, A, 0), "the block it serves stays");
    assert!(
        !set.no_pull_through(A),
        "a covered refusal is not a pull-through bar"
    );
    assert!(
        set.holder(B).is_some_and(|h| h.coverage.is_none()),
        "the whole holder is untouched"
    );

    set.merge(vec![partial(A, 10.0, &[0, 1])]);
    assert!(!covers(&set, A, 1), "a probe repeating the stale claim");
    assert!(covers(&set, A, 0));

    // A `NotFound` may be transient, so the drop ends with the bar's time,
    // and the count starts again.
    let later = at + super::PULL_THROUGH_BAR;
    set.expire_pull_through_bars(later);
    assert!(covers(&set, A, 1), "the drop ended");
    say_covered_not_found(&mut set, A, block1(false), 2, later);
    assert!(covers(&set, A, 1), "the count restarted with the drop");
}

/// Whether `provider`'s coverage in `set` holds `block`.
fn covers<P: SourceProvider>(set: &SourceSet<'_, P>, provider: Address, block: u32) -> bool {
    set.holder(provider)
        .and_then(|h| h.coverage.as_ref())
        .is_some_and(|c| c.covers(block))
}

/// Record `times` covered `NotFound` answers from `provider` for `range`,
/// a cooldown apart from `start`, as a lane that retries after each gives
/// them. Returns the time of the last one.
fn say_covered_not_found<P: SourceProvider>(
    set: &mut SourceSet<'_, P>,
    provider: Address,
    range: super::LaneRange,
    times: u32,
    start: Instant,
) -> Instant {
    let mut at = start;
    for n in 0..times {
        if n > 0 {
            at += crate::health::COOL_BASE;
        }
        say_not_found_in(set, provider, range, 1, at);
    }
    at
}

/// #2281: covered refusals spread out more than [`PULL_THROUGH_BAR`] apart,
/// as sparse load shedding gives, never add up to a drop.
///
/// [`PULL_THROUGH_BAR`]: super::PULL_THROUGH_BAR
#[tokio::test(start_paused = true)]
async fn sparse_covered_refusals_never_drop_a_block() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
    let mut at = Instant::now();
    for _ in 0..5 {
        say_not_found_in(&mut set, A, block1(false), 1, at);
        at += super::PULL_THROUGH_BAR;
    }
    assert!(covers(&set, A, 1));
}

/// #2281: one burst of covered refusals (the own stream and its extras
/// refused at one load-shed moment) counts once, and a verified byte in the
/// block clears its count.
#[tokio::test(start_paused = true)]
async fn a_burst_counts_once_and_a_served_byte_clears_the_count() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
    let now = Instant::now();
    say_not_found_in(&mut set, A, block1(false), 10, now);
    let at = say_covered_not_found(
        &mut set,
        A,
        block1(false),
        1,
        now + crate::health::COOL_BASE,
    );
    assert!(covers(&set, A, 1), "a burst and one more answer are two");

    set.record_covered_served(A, &[(64 << 20, 1024)]);
    say_covered_not_found(&mut set, A, block1(false), 2, at + crate::health::COOL_BASE);
    assert!(covers(&set, A, 1), "the served byte restarted the count");
}

/// #2281: a refusal mid-range counts only toward the blocks after the bytes
/// that landed: the node served the ones before.
#[tokio::test(start_paused = true)]
async fn a_mid_range_refusal_counts_only_the_blocks_after_what_landed() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
    let spanning = super::LaneRange {
        offset: 0,
        len: 128 << 20,
        landed: 64 << 20,
        past_end: false,
        uncovered: false,
    };
    say_covered_not_found(
        &mut set,
        A,
        spanning,
        super::ABSENT_AFTER_NOT_FOUND,
        Instant::now(),
    );
    assert!(covers(&set, A, 0), "block 0 landed");
    assert!(!covers(&set, A, 1));
}

/// #2281: a probe that reports a holder with a dropped block as whole ends
/// the drop, so its end does not turn the holder partial again.
#[tokio::test(start_paused = true)]
async fn a_holder_reported_whole_keeps_the_whole_blob_when_a_drop_ends() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0, 1])]);
    let now = Instant::now();
    let at = say_covered_not_found(
        &mut set,
        A,
        block1(false),
        super::ABSENT_AFTER_NOT_FOUND,
        now,
    );
    assert!(!covers(&set, A, 1));

    set.merge(vec![holder(A, 10.0)]);
    set.expire_pull_through_bars(at + super::PULL_THROUGH_BAR);
    assert!(
        set.holder(A).is_some_and(|h| h.coverage.is_none()),
        "the whole holder stays whole"
    );
}

/// An uncovered byte verified before the refusal that set the bar is
/// stale: a sibling worker that ends after the bar does not lift it.
#[tokio::test(start_paused = true)]
async fn an_uncovered_byte_from_before_the_bar_keeps_it() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let served_at = Instant::now();
    let refused_at = served_at + Duration::from_millis(5);
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        refused_at,
    );
    assert!(set.no_pull_through(A));
    set.record_pull_through(A, served_at);
    assert!(set.no_pull_through(A), "a byte from before the bar");
    set.record_pull_through(A, refused_at);
    assert!(set.no_pull_through(A), "a byte at the same instant");
    set.record_pull_through(A, refused_at + Duration::from_millis(1));
    assert!(!set.no_pull_through(A), "a byte after the bar");
}

/// The loop can take a worker's end after a sibling's: a refusal older
/// than an uncovered byte the loop already took is stale and does not
/// count, so the bar does not depend on which end the loop takes first.
#[tokio::test(start_paused = true)]
async fn a_refusal_older_than_a_taken_byte_does_not_count() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let refused_at = Instant::now();
    let served_at = refused_at + Duration::from_millis(5);
    set.record_pull_through(A, served_at);
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        refused_at,
    );
    assert!(
        !set.no_pull_through(A),
        "refusals before the byte are stale"
    );
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        served_at + Duration::from_millis(1),
    );
    assert!(set.no_pull_through(A), "refusals after the byte count");
}

/// A `NotFound` bar ends [`super::PULL_THROUGH_BAR`] after the refusal
/// that set it, and the count restarts: the holder takes an uncovered
/// range again, and [`super::ABSENT_AFTER_NOT_FOUND`] more refusals bar it
/// again. The loop wakes when a bar ends.
#[tokio::test(start_paused = true)]
async fn a_not_found_bar_ends_after_its_time_and_the_count_restarts() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let now = Instant::now();
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        now,
    );
    let ends = now + super::PULL_THROUGH_BAR;
    let just_before = now + super::PULL_THROUGH_BAR.saturating_sub(Duration::from_millis(1));
    assert_eq!(
        set.next_wake(just_before),
        Some(ends),
        "once its cooldown passes, the loop wakes when the bar ends"
    );
    set.expire_pull_through_bars(just_before);
    assert!(set.no_pull_through(A), "not yet");
    set.expire_pull_through_bars(ends);
    assert!(!set.no_pull_through(A), "the bar ends");
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND - 1,
        ends,
    );
    assert!(!set.no_pull_through(A), "the count restarted");
    say_not_found_in(&mut set, A, block1(true), 1, ends);
    assert!(set.no_pull_through(A), "barred again");
}

/// A size-ceiling bar on a partial holder stays for the blob: neither an
/// uncovered byte nor a rediscovery with wider coverage lifts it.
#[tokio::test(start_paused = true)]
async fn a_size_ceiling_bar_survives_pull_through_and_wider_coverage() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let now = Instant::now();
    set.record_fault(A, &too_large(), Some(block1(true)), now, U256::ZERO);
    assert!(set.no_pull_through(A));
    set.record_pull_through(A, now + Duration::from_millis(1));
    assert!(set.no_pull_through(A), "an uncovered byte keeps it");
    set.discovery_done(Ok(vec![partial(A, 10.0, &[0, 1])]), now, U256::ZERO);
    assert!(set.no_pull_through(A), "wider coverage keeps it");
    set.expire_pull_through_bars(now + super::PULL_THROUGH_BAR * 10);
    assert!(set.no_pull_through(A), "time does not end it");
}

/// A holder of the whole blob is never barred: no range lies outside its
/// coverage.
#[tokio::test(start_paused = true)]
async fn a_whole_holder_is_never_barred_from_pull_through() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found_in(&mut set, A, block1(true), 10, now);
    assert!(!set.no_pull_through(A));
}

/// A discovery that reports a block the barred holder did not cover
/// before lifts the bar. One that reports the same coverage keeps it.
#[tokio::test(start_paused = true)]
async fn wider_coverage_from_discovery_lifts_the_bar() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let now = Instant::now();
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        now,
    );
    set.discovery_done(Ok(vec![partial(A, 10.0, &[0])]), now, U256::ZERO);
    assert!(set.no_pull_through(A), "the same coverage keeps the bar");
    set.discovery_done(Ok(vec![partial(A, 10.0, &[0, 1])]), now, U256::ZERO);
    assert!(!set.no_pull_through(A), "block 1 is new");
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND - 1,
        now,
    );
    assert!(!set.no_pull_through(A), "the count starts again");
}

/// A barred holder ends the item, with the refusal that barred it, only
/// once no work left lies inside its coverage and a discovery found no
/// one else.
#[tokio::test(start_paused = true)]
async fn a_barred_holder_with_only_uncovered_work_left_ends_the_item() -> anyhow::Result<()> {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![partial(A, 10.0, &[0])]);
    let now = Instant::now();
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        now,
    );
    assert!(
        set.exhausted(U256::ZERO, true, true).is_none(),
        "no discovery since the bar"
    );
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    assert!(
        set.exhausted(U256::ZERO, true, true).is_none(),
        "a first bar may come from load shed: the holder gets its probe"
    );
    let later = now + super::PULL_THROUGH_BAR;
    set.expire_pull_through_bars(later);
    assert!(!set.no_pull_through(A), "the bar ends");
    say_not_found_in(
        &mut set,
        A,
        block1(true),
        super::ABSENT_AFTER_NOT_FOUND,
        later,
    );
    set.discovery_done(Ok(vec![]), later, U256::ZERO);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "work inside its coverage is left"
    );
    let err = set
        .exhausted(U256::ZERO, true, true)
        .ok_or_else(|| anyhow::anyhow!("only uncovered work is left"))?;
    assert!(
        err.downcast_ref::<super::NoSourceHasBlob>().is_some(),
        "{err:#}"
    );
    assert!(
        err.downcast_ref::<crate::UpstreamRefused>().is_some(),
        "{err:#}"
    );
    Ok(())
}

fn too_large() -> anyhow::Error {
    anyhow::Error::new(crate::UpstreamRefused::mid_stream(
        decdn_protocol::client::StreamError::BlobTooLarge,
    ))
}

/// A proxy that refuses the blob as larger than its size ceiling never
/// starts again for this blob, even once its cooldown ends and a
/// discovery reports it again, while the holder still starts. A set where
/// every source refused the blob as too large ends the item.
#[tokio::test(start_paused = true)]
async fn a_proxy_refusing_the_blob_as_too_large_is_excluded() -> anyhow::Result<()> {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![non_holder(A, 10.0), holder(B, 20.0)],
    );
    let now = Instant::now();
    let fault = set.record_fault(A, &too_large(), None, now, U256::ZERO);
    assert_eq!(fault, Fault::Source);
    set.discovery_done(
        Ok(vec![non_holder(A, 10.0), holder(B, 20.0)]),
        now,
        U256::ZERO,
    );
    let later = now + super::DISCOVERY_CAP;
    assert_eq!(
        set.next_to_start(later, U256::ZERO, &HashSet::new())
            .map(|h| h.provider),
        Some(B),
        "the holder starts and the proxy does not"
    );
    assert!(
        set.next_to_start(later, U256::ZERO, &HashSet::from([B]))
            .is_none(),
        "the proxy never starts again"
    );
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "B is left"
    );

    set.record_fault(B, &too_large(), None, later, U256::ZERO);
    set.discovery_done(Ok(vec![]), later, U256::ZERO);
    let err = set
        .exhausted(U256::ZERO, true, false)
        .ok_or_else(|| anyhow::anyhow!("every source refused the blob as too large"))?;
    assert!(
        err.downcast_ref::<super::NoSourceHasBlob>().is_some(),
        "{err:#}"
    );
    assert!(
        err.downcast_ref::<crate::UpstreamRefused>()
            .is_some_and(|r| matches!(
                r.error(),
                decdn_protocol::client::StreamError::BlobTooLarge
            )),
        "{err:#}"
    );
    Ok(())
}

/// A node applies its size ceiling only to a pull-through. So a partial
/// holder that refuses the blob as too large is barred from pull-through
/// only: once its cooldown ends it starts again for the blocks it covers.
/// A whole holder that refuses it never starts again.
#[tokio::test(start_paused = true)]
async fn a_partial_holder_refusing_the_blob_as_too_large_loses_only_pull_through() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![partial(A, 10.0, &[0]), holder(B, 20.0)],
    );
    let now = Instant::now();
    let fault = set.record_fault(A, &too_large(), Some(block1(true)), now, U256::ZERO);
    assert_eq!(fault, Fault::Source);
    assert!(set.no_pull_through(A), "one refusal bars its pull-through");

    let later = now + crate::health::COOL_CAP;
    assert_eq!(
        set.next_to_start(later, U256::ZERO, &HashSet::new())
            .map(|h| h.provider),
        Some(A),
        "the partial holder still starts for the blocks it covers"
    );
    set.discovery_done(Ok(vec![]), later, U256::ZERO);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "work inside its coverage is left"
    );

    set.record_fault(B, &too_large(), None, later, U256::ZERO);
    assert!(!set.no_pull_through(B), "a whole holder has no bar");
    let latest = later + crate::health::COOL_CAP;
    assert!(
        set.next_to_start(latest, U256::ZERO, &HashSet::from([A]))
            .is_none(),
        "the whole holder never starts again"
    );
}

/// The lane-fault line names the provider's record, and its level follows the
/// fault's class (#2331), because
/// `uncovered=false` alone does not say that the node holds the range: a
/// non-holder's lane covers the whole blob (#2339).
#[tokio::test(start_paused = true)]
async fn the_lane_fault_line_names_the_holder_record() {
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![non_holder(A, 10.0), partial(B, 20.0, &[0, 2, 3])],
    );
    let now = Instant::now();
    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        set.record_fault(A, &not_found(), Some(block0()), now, U256::ZERO);
        set.record_fault(B, &not_found(), Some(block0()), now, U256::ZERO);
        set.record_fault(B, &anyhow::anyhow!("reset"), None, now, U256::ZERO);
        let retry = anyhow::anyhow!("rpc timed out").context(crate::driver::TopUpFailed);
        set.record_fault(A, &retry, None, now, U256::ZERO);
    });
    let text = String::from_utf8_lossy(
        &log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
    .into_owned();
    let mut lines = text.lines();
    let a_line = lines.next().unwrap_or_default();
    let b_line = lines.next().unwrap_or_default();
    let b_source_line = lines.next().unwrap_or_default();
    let a_retry_line = lines.next().unwrap_or_default();
    assert!(lines.next().is_none(), "four fault lines: {text}");
    // A source fault is what an operator watches for (#2331); a chain-side
    // retry leaves the source's health untouched.
    for line in [a_line, b_line, b_source_line] {
        assert!(line.contains(" WARN "), "{text}");
    }
    assert!(a_retry_line.contains(" INFO "), "{text}");
    assert!(a_retry_line.contains("fault=Transient"), "{text}");
    assert!(a_line.contains("a lane faulted"), "{text}");
    assert!(a_line.contains(&format!("provider={A}")), "{text}");
    assert!(a_line.contains("uncovered=false"), "{text}");
    assert!(a_line.contains("probed_holder=false"), "{text}");
    assert!(a_line.contains("coverage=whole blob"), "{text}");
    assert!(b_line.contains("a lane faulted"), "{text}");
    assert!(b_line.contains(&format!("provider={B}")), "{text}");
    assert!(b_line.contains("probed_holder=true"), "{text}");
    assert!(b_line.contains("coverage=0,2-3"), "{text}");
    assert!(b_source_line.contains("a source faulted"), "{text}");
    assert!(b_source_line.contains("coverage=0,2-3"), "{text}");
}

/// A bound that overshoots the blob sends pieces past its true end, and an
/// honest node refuses them with `NotFound`. Such overshoot refusals from a
/// non-holder cool it but never mark it absent, so the item does not end
/// with a wrong "check the hash".
#[tokio::test(start_paused = true)]
async fn overshoot_refusals_never_mark_a_non_holder_absent() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    let overshoot = super::LaneRange {
        offset: 64 << 20,
        len: 64 << 20,
        landed: 0,
        past_end: true,
        uncovered: false,
    };
    for _ in 0..10 {
        let fault = set.record_fault(A, &not_found(), Some(overshoot), now, U256::ZERO);
        assert_eq!(fault, Fault::Source);
    }
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "overshoot refusals never mark the node absent"
    );
    assert!(
        set.health().cooling_until(A, now).is_some(),
        "the node cools"
    );
}

/// A non-holder that says `NotFound` three times ends the item, and the
/// stop names the refusal: it downcasts to both the stop and the refusal.
#[tokio::test(start_paused = true)]
async fn a_non_holder_saying_not_found_three_times_ends_the_item() -> anyhow::Result<()> {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND - 1, now);
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    assert!(
        set.exhausted(U256::ZERO, true, false).is_none(),
        "two answers are not enough"
    );
    say_not_found(&mut set, A, 1, now);
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    let err = set
        .exhausted(U256::ZERO, true, false)
        .ok_or_else(|| anyhow::anyhow!("three answers end the item"))?;
    assert!(
        err.downcast_ref::<super::NoSourceHasBlob>().is_some(),
        "{err:#}"
    );
    assert!(
        err.downcast_ref::<crate::UpstreamRefused>().is_some(),
        "{err:#}"
    );
    assert_eq!(
        crate::fault::classify(&err),
        Fault::Fatal(crate::fault::FatalScope::Item)
    );
    Ok(())
}

/// A verified byte resets a non-holder's `NotFound` count.
#[tokio::test(start_paused = true)]
async fn a_byte_between_not_found_answers_resets_the_count() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, 2, now);
    set.record_progress(A);
    say_not_found(&mut set, A, 2, now);
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    assert!(set.exhausted(U256::ZERO, true, false).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_new_holder_from_discovery_keeps_the_item_alive() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
    set.discovery_done(Ok(vec![holder(B, 5.0)]), now, U256::ZERO);
    assert!(set.exhausted(U256::ZERO, true, false).is_none());
}

/// A discovery whose probe now reports an absent provider as a holder
/// clears its absent mark.
#[tokio::test(start_paused = true)]
async fn a_probe_that_reports_the_blob_clears_an_absent_mark() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
    set.discovery_done(Ok(vec![holder(A, 10.0)]), now, U256::ZERO);
    assert!(set.exhausted(U256::ZERO, true, false).is_none());
}

#[tokio::test(start_paused = true)]
async fn progress_clears_a_not_found_mark() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
    set.record_progress(A);
    set.discovery_done(Ok(vec![]), now, U256::ZERO);
    assert!(set.exhausted(U256::ZERO, true, false).is_none());
}

/// A discovery that fails while the set is unanimous must still back off:
/// it must not spin every tick just because every source is marked absent.
#[tokio::test(start_paused = true)]
async fn a_failed_discovery_backs_off_even_while_unanimous() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![non_holder(A, 10.0), non_holder(B, 20.0)],
    );
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
    say_not_found(&mut set, B, super::ABSENT_AFTER_NOT_FOUND, now);
    set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
    assert!(!set.wants_discovery(now, U256::ZERO, 0, false));
    assert!(!set.wants_discovery(
        now + DISCOVERY_BASE - Duration::from_millis(1),
        U256::ZERO,
        0,
        false
    ));
    assert!(set.wants_discovery(now + DISCOVERY_BASE, U256::ZERO, 0, false));
}

/// A unanimously stuck set keeps wanting a fresh discovery even once an
/// individual source's delivery cooldown clears and it looks startable
/// again: the `starved` gate must not mask the unanimous verdict.
#[tokio::test(start_paused = true)]
async fn a_unanimous_absent_set_still_wants_discovery_once_cooldowns_clear() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![non_holder(A, 10.0)]);
    let now = Instant::now();
    say_not_found(&mut set, A, super::ABSENT_AFTER_NOT_FOUND, now);
    set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
    // Three faults in a row cool A until 8 s after `now`.
    let later = now + super::DISCOVERY_CAP;
    // A's delivery cooldown is long over by `later`, so it is
    // startable again and `running == 0` no longer implies "starved".
    assert!(
        set.next_to_start(later, U256::ZERO, &HashSet::new())
            .is_some(),
        "the source itself is startable again"
    );
    assert!(
        set.wants_discovery(later, U256::ZERO, 1, false),
        "but the set is still unanimously absent, so discovery must still be wanted"
    );
}

/// A mixed set — one source priced out, another saying it does not hold
/// the blob — still ends the command once the top-up budget is spent: an
/// absent source is excluded from the affordability verdict too, and at
/// least one source being priced out is what makes it a command-ending
/// `NoAffordableSource` rather than the pure "nobody has it" case.
#[tokio::test(start_paused = true)]
async fn a_mixed_unaffordable_and_absent_set_stops_once_topups_are_spent() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(
        &p,
        [0; 32],
        Arc::default(),
        vec![holder(A, 10.0), non_holder(B, 20.0)],
    );
    let now = Instant::now();
    let dep = U256::from(100u64);
    let dry = anyhow::Error::new(crate::driver::PoolExhausted {
        gap_start: 0,
        gap_len: 1,
    });
    set.record_fault(A, &dry, None, now, dep);
    for _ in 0..super::ABSENT_AFTER_NOT_FOUND {
        set.record_fault(B, &not_found(), None, now, dep);
    }
    set.discovery_done(Ok(vec![]), now, dep);
    assert!(set.exhausted(dep, true, false).is_none(), "top-ups left");
    let err = set.exhausted(dep, false, false);
    assert!(err.is_some_and(|e| e.downcast_ref::<NoAffordableSource>().is_some()));
}

/// A discovery that brings in a genuinely new provider resets the backoff,
/// so the set looks again soon rather than waiting out the doubled wait.
#[tokio::test(start_paused = true)]
async fn discovering_a_new_holder_resets_the_backoff() {
    let p = provider(vec![]);
    let mut set = SourceSet::new(&p, [0; 32], Arc::default(), vec![holder(A, 10.0)]);
    let now = Instant::now();
    set.discovery_done(Err(anyhow::anyhow!("registry rpc down")), now, U256::ZERO);
    assert_eq!(set.next_wake(now), Some(now + DISCOVERY_BASE));
    set.discovery_done(Ok(vec![holder(B, 5.0)]), now, U256::ZERO);
    assert_eq!(
        set.next_wake(now),
        None,
        "a fresh holder resets the discovery timer"
    );
}
