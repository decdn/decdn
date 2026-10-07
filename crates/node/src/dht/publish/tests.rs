use super::*;

fn h(b: u8) -> ContentHash {
    ContentHash::from_bytes([b; 32])
}

fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}

/// A routing table holding `peers`, behind the lock `plan_cycle` takes.
fn table_of(peers: &[u8]) -> Mutex<RoutingTable> {
    let mut table = RoutingTable::new(nid(0x01));
    for &b in peers {
        table.insert(nid(b));
    }
    Mutex::new(table)
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap()
}

#[test]
fn a_planned_cycle_groups_every_hash_under_each_of_its_closest_peers() {
    // A table with fewer than REPUBLISH_FANOUT (23) peers means every
    // peer is within the K+3 closest set of every hash, so each peer's
    // set must carry every drained hash exactly once — the grouping the
    // tick path hands to `publish_batch`.
    let routing = table_of(&[0x10, 0x20, 0x30]);
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(0xA0), now);
    place(&s, h(0xB0), now + 1);
    let (drained, budget) = plan_cycle(&s, &routing, now, &mut None);
    assert_eq!(drained, vec![h(0xA0), h(0xB0)]);
    let groups = budget.into_groups(&drained.iter().copied().collect());
    assert_eq!(
        groups.len(),
        3,
        "all three peers are closest to both hashes"
    );
    for b in [0x10, 0x20, 0x30] {
        assert_eq!(
            groups.get(&nid(b)),
            Some(&drained),
            "peer {b:#x} must receive every drained hash once"
        );
    }
}

#[test]
fn a_planned_cycle_on_an_empty_table_groups_nothing() {
    // No routing-table peers (e.g. boot before bootstrap): the due record
    // drains, nothing is pulled forward, and there is no batch to send.
    let routing = table_of(&[]);
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(0xA0), now);
    place(&s, h(0xB0), now + 1);
    let (drained, budget) = plan_cycle(&s, &routing, now, &mut None);
    assert_eq!(drained, vec![h(0xA0)]);
    assert!(
        budget
            .into_groups(&drained.into_iter().collect())
            .is_empty()
    );
}

#[test]
fn a_planned_cycle_reaches_exactly_the_lookahead_window() {
    let routing = table_of(&[0x10]);
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    let window = micros(REPUBLISH_LOOKAHEAD);
    place(&s, h(1), now);
    place(&s, h(2), now + window - 1);
    place(&s, h(3), now + window + 1);
    let (drained, _) = plan_cycle(&s, &routing, now, &mut None);
    assert_eq!(drained, vec![h(1), h(2)]);
    assert_eq!(s.len(), 1, "the record past the window stays scheduled");
}

#[test]
fn a_planned_cycle_takes_no_lookahead_right_after_another_cycle() {
    // The cap bounds one cycle, not two a second apart: a look-ahead
    // batch right behind a cycle that emptied a receiver's bucket would
    // have its tail refused.
    let routing = table_of(&[0x10]);
    let s = RepublishScheduler::new();
    let t0: u64 = 1_000_000_000;
    let spacing = micros(LOOKAHEAD_MIN_SPACING);
    place(&s, h(1), t0);
    place(&s, h(2), t0 + 1_000_000);
    place(&s, h(3), t0 + 1_000_001);
    // Due only after the third cycle, so only the look-ahead takes them.
    let t3 = t0 + 1_000_000 + spacing;
    place(&s, h(4), t3 + 10);
    place(&s, h(5), t3 + 11);
    let mut last = Some(t0 - 1);
    assert_eq!(
        plan_cycle(&s, &routing, t0, &mut last).0,
        vec![h(1)],
        "a cycle 1 µs after the last one takes only its due record"
    );
    assert_eq!(last, Some(t0), "a non-empty cycle moves the mark");
    assert_eq!(
        plan_cycle(&s, &routing, t0 + 1_000_000, &mut last).0,
        vec![h(2)],
        "1 s later: still too close, due records only"
    );
    assert_eq!(
        plan_cycle(&s, &routing, t3, &mut last).0,
        vec![h(3), h(4), h(5)],
        "a full spacing later the look-ahead runs again"
    );
}

#[test]
fn a_due_set_of_only_tombstones_does_not_unlock_the_lookahead() {
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(1), now);
    s.unschedule(&h(1));
    place(&s, h(2), now + 1);
    assert!(s.drain_cycle(now, now + 10, |_, _| true).is_empty());
    assert_eq!(s.len(), 1);
}

#[test]
fn the_receiver_cap_fits_the_default_per_peer_burst() {
    // A full look-ahead batch must fit the burst a receiver on the
    // ADR 022 defaults grants one publisher.
    let burst = crate::dht::rate_limit::DhtRateLimitConfig::default().per_peer_burst;
    assert!(LOOKAHEAD_RECEIVER_CAP < usize::try_from(burst).unwrap());
}

#[test]
fn schedule_steady_adds_one_entry() {
    let s = RepublishScheduler::new();
    assert!(s.is_empty());
    s.schedule_steady(h(1));
    assert_eq!(s.len(), 1);
}

#[test]
fn unschedule_removes_from_set() {
    let s = RepublishScheduler::new();
    s.schedule_steady(h(1));
    s.unschedule(&h(1));
    assert!(s.is_empty());
}

#[test]
fn drain_due_returns_only_past_entries() {
    let s = RepublishScheduler::new();
    // Steady-state min is 30 min — schedule one steady-state and
    // one explicit "now" entry; the explicit one should drain,
    // the steady-state one should not.
    s.schedule_with_offset(h(1), 0); // due immediately
    s.schedule_steady(h(2));
    let drained = s.drain_due(now_us());
    assert_eq!(drained, vec![h(1)]);
    // h(2) still scheduled.
    assert_eq!(s.len(), 1);
}

#[test]
fn drain_due_filters_via_scheduled_set() {
    // Schedule h(1), then unschedule before drain — the heap entry
    // should be dropped silently rather than republished.
    let s = RepublishScheduler::new();
    s.schedule_with_offset(h(1), 0);
    s.unschedule(&h(1));
    let drained = s.drain_due(now_us());
    assert!(drained.is_empty());
}

/// Place `hash` at an exact `due_us`, so a look-ahead test controls the
/// order and spacing of due times rather than drawing them from jitter.
fn place(s: &RepublishScheduler, hash: ContentHash, due_us: u64) {
    let (mut heap, mut scheduled) = (s.heap.lock().unwrap(), s.scheduled.lock().unwrap());
    scheduled.insert(hash, due_us);
    schedule_at(&mut heap, hash, due_us);
}

/// A budget where every hash has the one receiver `nid(0x99)` — the
/// N ≤ K+3 shape, where every hash goes to every peer.
fn one_receiver(budget: &mut ReceiverBudget) -> impl FnMut(&ContentHash, bool) -> bool + '_ {
    |hash, mandatory| budget.admit(*hash, &[nid(0x99)], mandatory)
}

#[test]
fn lookahead_does_not_run_on_a_tick_with_nothing_due() {
    // Pulling records forward on an idle tick would only shift the whole
    // schedule earlier and batch nothing.
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(1), now + 1);
    place(&s, h(2), now + 2);
    let mut budget = ReceiverBudget::default();
    assert!(
        s.drain_cycle(now, now + 10, one_receiver(&mut budget))
            .is_empty()
    );
    assert_eq!(s.len(), 2);
}

#[test]
fn lookahead_takes_records_due_by_the_horizon_in_due_order() {
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(3), now + 5);
    place(&s, h(1), now);
    place(&s, h(2), now + 3);
    place(&s, h(4), now + 11); // past the horizon
    let mut budget = ReceiverBudget::default();
    assert_eq!(
        s.drain_cycle(now, now + 10, one_receiver(&mut budget)),
        vec![h(1), h(2), h(3)]
    );
    assert_eq!(s.len(), 1, "the record past the horizon stays scheduled");
}

#[test]
fn lookahead_stops_at_the_receiver_cap_and_leaves_the_rest_scheduled() {
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(0), now);
    for b in 1u8..=40 {
        place(&s, h(b), now + u64::from(b));
    }
    let mut budget = ReceiverBudget::default();
    let drained = s.drain_cycle(now, now + 100, one_receiver(&mut budget));
    let want: Vec<ContentHash> = (0u8..32).map(h).collect();
    assert_eq!(drained, want, "the due record plus 31 pulled forward");
    assert_eq!(s.len(), 9, "the refused record and every later one stay");

    // They still drain at their own due times.
    let mut budget = ReceiverBudget::default();
    assert_eq!(
        s.drain_cycle(now + 40, now + 40, one_receiver(&mut budget)),
        (32u8..=40).map(h).collect::<Vec<_>>()
    );
}

#[test]
fn due_records_past_the_cap_still_drain_but_take_no_lookahead() {
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    for b in 0u8..40 {
        place(&s, h(b), now.saturating_sub(u64::from(b)));
    }
    place(&s, h(200), now + 1);
    let mut budget = ReceiverBudget::default();
    assert_eq!(
        s.drain_cycle(now, now + 10, one_receiver(&mut budget))
            .len(),
        40
    );
    assert_eq!(s.len(), 1, "the look-ahead record waits for its due time");
}

#[test]
fn a_tombstone_in_the_lookahead_window_is_skipped_and_not_charged() {
    let s = RepublishScheduler::new();
    let now: u64 = 1_000_000_000;
    place(&s, h(1), now);
    place(&s, h(2), now + 1);
    s.unschedule(&h(2));
    place(&s, h(3), now + 2);
    let mut charged = Vec::new();
    let drained = s.drain_cycle(now, now + 10, |hash, _| {
        charged.push(*hash);
        true
    });
    assert_eq!(drained, vec![h(1), h(3)]);
    assert_eq!(charged, vec![h(1), h(3)]);
}

#[test]
fn receiver_budget_caps_each_receiver_independently() {
    let mut budget = ReceiverBudget::default();
    for b in 0..LOOKAHEAD_RECEIVER_CAP {
        assert!(budget.admit(h(u8::try_from(b).unwrap()), &[nid(0xA)], false));
    }
    assert!(!budget.admit(h(0xE0), &[nid(0xA)], false), "A is full");
    assert!(
        !budget.admit(h(0xE1), &[nid(0xA), nid(0xB)], false),
        "one full receiver refuses the hash"
    );
    assert!(
        budget.admit(h(0xE2), &[nid(0xB)], false),
        "B still has room"
    );
    assert!(
        budget.admit(h(0xE3), &[nid(0xA)], true),
        "a due record ignores the cap"
    );
    let groups = budget.into_groups(&(0u8..=0xFF).map(h).collect());
    assert_eq!(groups[&nid(0xA)].len(), LOOKAHEAD_RECEIVER_CAP + 1);
    assert_eq!(
        groups[&nid(0xB)],
        vec![h(0xE2)],
        "a refused hash joins no set, not even a receiver with room"
    );
}

#[test]
fn receiver_budget_refuses_lookahead_with_no_receivers() {
    let mut budget = ReceiverBudget::default();
    assert!(!budget.admit(h(1), &[], false));
    assert!(budget.admit(h(2), &[], true));
    assert!(
        budget
            .into_groups(&[h(1), h(2)].into_iter().collect())
            .is_empty()
    );
}

#[test]
fn receiver_budget_groups_drop_hashes_the_due_time_gate_removed() {
    // An evicted or faulted hash leaves every set, and a receiver whose
    // set empties gets no batch.
    let mut budget = ReceiverBudget::default();
    assert!(budget.admit(h(1), &[nid(0xA), nid(0xB)], true));
    assert!(budget.admit(h(2), &[nid(0xB)], true));
    let groups = budget.into_groups(&std::iter::once(h(2)).collect());
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[&nid(0xB)], vec![h(2)]);
}

#[test]
fn cold_start_jitter_bounded_by_window() {
    // Drawing 100 cold-start offsets must all land in [0, COLD_START_MAX].
    for _ in 0..100 {
        let j = jitter_us(Duration::ZERO, COLD_START_MAX);
        assert!(j <= u64::try_from(COLD_START_MAX.as_micros()).unwrap());
    }
}

#[test]
fn seed_cold_start_schedules_every_hash_within_cold_start_window() {
    // Seeding N hashes returns N, and every hash drains within the
    // cold-start window plus a small slack. Behavioural check via
    // `drain_due` rather than peeking at the heap so the test
    // survives a future switch to a different scheduling primitive.
    let s = RepublishScheduler::new();
    let hashes: Vec<ContentHash> = (1u8..=5).map(h).collect();
    let n = s.seed_cold_start(hashes.iter().copied());
    assert_eq!(n, 5);
    assert_eq!(s.len(), 5);
    let deadline_us = now_us()
        .saturating_add(u64::try_from(COLD_START_MAX.as_micros()).unwrap())
        .saturating_add(1_000); // 1 ms slack for drift between seed and check
    let drained: HashSet<ContentHash> = s.drain_due(deadline_us).into_iter().collect();
    let expected: HashSet<ContentHash> = hashes.into_iter().collect();
    assert_eq!(drained, expected);
    assert!(
        s.is_empty(),
        "drain_due at deadline should empty the scheduler"
    );
}

/// Single-blob stub origin.
///
/// Three independent axes, because the real backends differ on exactly
/// these: `fetch` is what puts a hash in the store; `size` is what
/// `origin_probe_presence` asks (the ownership test); `enumerate` plus
/// `size` is what puts it in the origin-held index. A filesystem origin
/// does all three; a remote S3/R2/HTTP origin answers `size` but
/// deliberately does not enumerate.
#[derive(Debug)]
struct StubOrigin {
    data: bytes::Bytes,
    hash: decdn_cache::Hash,
    axes: StubAxes,
}

/// The four axes real backends differ on, as one value so a constructor
/// cannot set three of them and forget the fourth.
///
/// Four independent booleans is what the fixture is: each axis is a distinct
/// thing a real backend does or does not do, and every combination models a
/// backend that exists. `Default` is "holds nothing, lists nothing, fails
/// nothing", so each constructor names only what it varies.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Default, Clone, Copy)]
struct StubAxes {
    /// `enumerate` lists the blob, which is what puts it in the origin-held
    /// index.
    enumerable: bool,
    /// `size` confirms the blob — the ownership test.
    answers_size: bool,
    /// `size` returns a transport error instead of an answer, because a
    /// backend that is merely unreachable must not read as one that does not
    /// hold the object.
    faults_size: bool,
    /// `enumerate` fails, so the rescan gets no candidates from this origin
    /// at all — the coarser half of the same outage.
    faults_enumerate: bool,
}

impl decdn_cache::Origin for StubOrigin {
    fn kind(&self) -> decdn_cache::OriginKind {
        decdn_cache::OriginKind::Filesystem
    }

    fn fetch(
        &self,
        hash: decdn_cache::Hash,
        _max_bytes: u64,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>>
                + Send
                + '_,
        >,
    > {
        let result = if hash == self.hash {
            Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
        } else {
            Ok(decdn_cache::OriginFetch::NotFound)
        };
        Box::pin(async move { result })
    }

    fn size(
        &self,
        hash: decdn_cache::Hash,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<Option<u64>, decdn_cache::OriginPullError>> + Send + '_>,
    > {
        if self.axes.faults_size {
            return Box::pin(async {
                Err(decdn_cache::OriginPullError::Transient(anyhow::anyhow!(
                    "synthetic HEAD outage"
                )))
            });
        }
        let n = (self.axes.answers_size && hash == self.hash)
            .then(|| u64::try_from(self.data.len()).unwrap_or(u64::MAX));
        Box::pin(async move { Ok(n) })
    }

    fn enumerate(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<Vec<decdn_cache::Hash>, decdn_cache::OriginPullError>>
                + Send
                + '_,
        >,
    > {
        if self.axes.faults_enumerate {
            return Box::pin(async {
                Err(decdn_cache::OriginPullError::Transient(anyhow::anyhow!(
                    "synthetic listing outage"
                )))
            });
        }
        let out = if self.axes.enumerable {
            vec![self.hash]
        } else {
            Vec::new()
        };
        Box::pin(async move { Ok(out) })
    }
}

/// A filesystem-shaped origin: enumerates and answers `size`, so its blob
/// lands in the origin-held index.
fn stub_origin(payload: &'static [u8], enumerable: bool) -> Arc<dyn decdn_cache::Origin> {
    Arc::new(StubOrigin {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
        axes: StubAxes {
            enumerable,
            answers_size: enumerable,
            ..StubAxes::default()
        },
    })
}

/// A remote-shaped origin: answers `size` (so it is this node's own
/// content) but never enumerates, so nothing puts it in the origin-held
/// index. Unpinned objects on S3/R2/HTTP have exactly this shape.
fn remote_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
    Arc::new(StubOrigin {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
        axes: StubAxes {
            answers_size: true,
            ..StubAxes::default()
        },
    })
}

/// An origin that cannot be listed and cannot answer `size` — a backend that
/// has gone away entirely, so the rescan reports both of its legs.
fn enumerating_faulting_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
    Arc::new(StubOrigin {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
        axes: StubAxes {
            enumerable: true,
            answers_size: true,
            faults_size: true,
            faults_enumerate: true,
        },
    })
}

/// A remote-shaped origin that has gone unreachable: `size` returns a
/// transport error, so the ownership test can neither confirm nor deny.
fn faulting_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
    Arc::new(StubOrigin {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
        axes: StubAxes {
            answers_size: true,
            faults_size: true,
            ..StubAxes::default()
        },
    })
}

/// An origin that holds nothing: no enumerate, no size. Content fetched
/// through it is foreign relay traffic, not this node's own.
fn foreign_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
    Arc::new(StubOrigin {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
        axes: StubAxes::default(),
    })
}

/// With relay on, the snapshot is the union of both halves — and a hash
/// present in both appears once, so it draws one jitter offset rather
/// than two.
#[tokio::test]
async fn holder_snapshot_unions_store_and_origin_held() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    // `stored` is fetchable but not enumerable (store half only);
    // `origin` is enumerable and also fetched below, so it lands in both.
    let stored = decdn_cache::Hash::new(b"holder-stored");
    let origin_held = decdn_cache::Hash::new(b"holder-origin");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![
            foreign_stub_origin(b"holder-stored"),
            stub_origin(b"holder-origin", true),
        ],
        16,
    )
    .await?;
    cache.get(stored).await?;
    cache.get(origin_held).await?;
    cache.rescan_origins().await;

    let snap = holder_snapshot(&cache, true).await;

    assert!(snap.store_error.is_none(), "the store walk must succeed");
    assert!(snap.hashes.contains(&stored), "store half missing");
    assert!(
        snap.hashes.contains(&origin_held),
        "origin-held half missing"
    );
    assert_eq!(snap.hashes.len(), 2, "the overlap must dedupe: {snap:?}");
    Ok(())
}

/// Origin-only nodes must not announce foreign store content: the store can
/// hold what was relayed before the toggle was set, and the serve gate now
/// declines it.
#[tokio::test]
async fn holder_snapshot_skips_foreign_store_content_under_origin_only() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let foreign = decdn_cache::Hash::new(b"holder-foreign");
    let own = decdn_cache::Hash::new(b"holder-own");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![
            foreign_stub_origin(b"holder-foreign"),
            stub_origin(b"holder-own", true),
        ],
        16,
    )
    .await?;
    cache.get(foreign).await?;
    cache.get(own).await?;
    cache.rescan_origins().await;

    let snap = holder_snapshot(&cache, false).await;

    assert!(
        !snap.hashes.contains(&foreign),
        "an origin-only node must not announce store-only content"
    );
    assert!(
        snap.hashes.contains(&own),
        "own (origin-held) content is announced regardless of the toggle"
    );
    assert!(
        snap.store_error.is_none(),
        "a skipped store walk is not a failed one"
    );
    Ok(())
}

/// ...but it MUST still announce its own store-only content.
///
/// The origin-held index covers enumerable origins plus present pins, and a
/// remote origin does not enumerate. An unpinned object this node owns,
/// committed to the store with its insert event dropped, is in neither half
/// unless the ownership probe puts it back — which is the whole recovery
/// this sweep promises for an origin-only node.
#[tokio::test]
async fn holder_snapshot_recovers_own_store_only_content_under_origin_only() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let mine = decdn_cache::Hash::new(b"policy-remote-own");
    let theirs = decdn_cache::Hash::new(b"policy-relayed");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![
            remote_stub_origin(b"policy-remote-own"),
            foreign_stub_origin(b"policy-relayed"),
        ],
        16,
    )
    .await?;
    cache.get(mine).await?;
    cache.get(theirs).await?;
    cache.rescan_origins().await;

    // Neither hash is in the origin-held index: the remote origin does not
    // enumerate, and neither is pinned. Without the ownership probe the
    // snapshot would be empty.
    assert!(
        cache.origin_held_snapshot().hashes.is_empty(),
        "fixture precondition: nothing is enumerable or pinned"
    );

    let snap = holder_snapshot(&cache, false).await;

    assert!(
        snap.hashes.contains(&mine),
        "an origin-only node must recover its own store-only content"
    );
    assert!(
        !snap.hashes.contains(&theirs),
        "relayed content is not this node's to announce"
    );
    Ok(())
}

/// An ownership probe that faults must be reported, not silently skipped.
///
/// Under the origin-only policy every stored hash is put to the origin to
/// decide whether it is this node's to announce. A transport blip answers
/// neither way, and admitting it would advertise content the serve gate
/// might then refuse — so the hash is left out. Left out and unreported, an
/// unreachable remote origin silently shrinks the announce set of a node
/// that holds the content, which is the same failure the rescan half fixes.
#[tokio::test]
async fn holder_snapshot_reports_a_faulted_ownership_probe() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let unreachable = decdn_cache::Hash::new(b"policy-probe-fault");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![faulting_stub_origin(b"policy-probe-fault")],
        16,
    )
    .await?;
    cache.get(unreachable).await?;

    let snap = holder_snapshot(&cache, false).await;

    assert!(
        !snap.hashes.contains(&unreachable),
        "a fault is not a confirmation; announcing on one risks advertising \
         content the serve gate refuses"
    );
    assert_eq!(
        snap.ownership_probe_faults, 1,
        "the skipped hash must be reported, or the snapshot reads healthy \
         while the node holds content it does not announce"
    );

    // The sweep's completion line must name it as degraded, same as a
    // store-walk failure.
    let scheduler = RepublishScheduler::new();
    let metrics = crate::metrics::Metrics::new();
    let outcome = lag_sweep(&cache, false, &scheduler, &metrics).await;
    assert!(
        outcome.degraded,
        "a sweep that could not resolve everything it holds is degraded"
    );
    Ok(())
}

/// The rescan's own unresolved counts must reach the seed, not just the
/// walk's.
///
/// `holder_snapshot` seeds both counters from the cache's report and then
/// adds its own ownership-probe faults on top. Without this, replacing
/// either initializer with a literal `0` passes every other test while
/// silently disabling the rescan half of the degradation signal end to end —
/// including the cold-start warn and `SweepOutcome::degraded`.
#[tokio::test]
async fn holder_snapshot_carries_the_rescans_own_failures() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    // Enumerable, so the rescan asks about it — and faulting, so it cannot
    // answer. The listing itself fails too, which is the other leg.
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![enumerating_faulting_stub_origin(b"rescan-wiring")],
        16,
    )
    .await?;
    cache.rescan_origins().await;

    let snap = holder_snapshot(&cache, true).await;
    assert_eq!(
        snap.rescan_enumerate_failures, 1,
        "the rescan's failed listing must reach the seed"
    );

    let scheduler = RepublishScheduler::new();
    let metrics = crate::metrics::Metrics::new();
    let outcome = lag_sweep(&cache, true, &scheduler, &metrics).await;
    assert!(
        outcome.degraded,
        "a seed built on a rescan that could not list its origin is degraded"
    );
    Ok(())
}

/// `cache_still_holds` gates every due entry. Origin-held content is never
/// imported into the iroh-blobs store, so a store-only check would drop it
/// at its first due time — advertised at probe time, never published.
#[tokio::test]
async fn cache_still_holds_accepts_origin_held_content() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin_only = decdn_cache::Hash::new(b"held-origin-only");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![stub_origin(b"held-origin-only", true)],
        16,
    )
    .await?;
    // Deliberately no `get`: the blob stays out of the store.
    cache.rescan_origins().await;
    assert!(
        !cache.has(origin_only).await?,
        "fixture precondition: the blob is not in the store"
    );

    let hash = ContentHash::from_bytes(*origin_only.as_bytes());
    assert_eq!(
        cache_still_holds(&cache, &hash).await,
        Some(true),
        "origin-held content must survive the due-time gate"
    );

    let absent = ContentHash::from_bytes(*decdn_cache::Hash::new(b"held-nowhere").as_bytes());
    assert_eq!(
        cache_still_holds(&cache, &absent).await,
        Some(false),
        "content held nowhere must still be dropped"
    );
    Ok(())
}

/// Deterministic pseudo-random plaintext plus its bao pre-order outboard,
/// mirroring `decdn_cache::engine::tests::synth_blob` — that helper is
/// private to the `cache` crate, so this is a from-scratch build over
/// the same public `bao_tree` primitive.
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, bytes::Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes()[0];
    }
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        &plaintext,
        decdn_bao_range::IROH_BLOCK_SIZE,
    );
    (*ob.root.as_bytes(), plaintext, bytes::Bytes::from(ob.data))
}

/// A verified, ready-to-`admit_bao` bao encoding of `[off, off+len)` of a
/// `total`-byte blob rooted at `root`, using the public
/// `decdn_bao_range::{align_range, encode_verified_range}` seam — the same
/// verification path a real ranged pull goes through, just fed synthetic
/// content instead of a network origin. Mirrors
/// `decdn_cache::engine::tests::bao_for` (also private to `cache`).
fn bao_for(
    root: [u8; 32],
    plaintext: &[u8],
    outboard: bytes::Bytes,
    off: u64,
    len: u64,
    total: u64,
) -> (decdn_cache::Hash, bao_tree::ChunkRanges, bytes::Bytes) {
    let aligned = decdn_bao_range::align_range(off, len, total).expect("range within blob");
    let s = usize::try_from(aligned.fetch_start()).expect("fetch_start fits usize");
    let e = usize::try_from(aligned.fetch_end()).expect("fetch_end fits usize");
    let encoded = decdn_bao_range::encode_verified_range(
        root,
        &aligned,
        plaintext.get(s..e).expect("aligned range within plaintext"),
        outboard,
    )
    .expect("synthetic range verifies against its own root");
    (
        decdn_cache::Hash::from(root),
        aligned.chunk_ranges().clone(),
        encoded,
    )
}

/// The advertise-gate relax (#1506): `cache_still_holds` must admit a
/// PARTIAL holder — one that has verified at least one
/// [`decdn_protocol::DISCOVERY_BLOCK_BYTES`] discovery block but is
/// neither `Complete` nor origin-held — into the announce set. Before the
/// relax, `cache_still_holds` asked `CacheEngine::has` (`Complete`-only),
/// which would have answered `Some(false)` for exactly this fixture: the
/// blob spans two discovery blocks and only the first is admitted.
#[tokio::test]
async fn cache_still_holds_accepts_a_partial_holder_with_at_least_one_verified_block()
-> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let cache = decdn_cache::CacheEngine::open(tmp.path(), vec![], 16).await?;

    // Two discovery blocks: block 0 is admitted whole, block 1's middle
    // group is left missing — a genuine partial, not a rounding
    // artifact. The trailing group is admitted too, so block 1's middle
    // group is its only gap.
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * group;
    let (root, plaintext, outboard) =
        synth_blob(usize::try_from(total).expect("test blob size fits usize"));
    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard.clone(),
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );
    cache.admit_bao(hash, block0_ranges, block0_bao).await?;
    let (_, tail_ranges, tail_bao) =
        bao_for(root, &plaintext, outboard, total - group, group, total);
    cache.admit_bao(hash, tail_ranges, tail_bao).await?;

    assert!(
        !cache.present_ranges(hash).await?.is_complete(),
        "fixture precondition: block 1's middle group was never admitted, \
         so this is a genuine partial"
    );
    let cov = cache.coverage(hash).await?;
    assert!(!cov.is_empty(), "fixture precondition: block 0 is covered");
    assert!(
        !cov.covers(1),
        "fixture precondition: block 1 must NOT be covered"
    );
    // This is what makes the fixture prove the relax: under the OLD
    // `cache.has()` (`Complete`-only) gate, `cache_still_holds` would
    // have answered `Some(false)` for this exact holder.
    assert!(
        !cache.has(hash).await?,
        "fixture precondition: this holder is NOT Complete — the old gate \
         would have dropped it"
    );

    let content_hash = ContentHash::from_bytes(*hash.as_bytes());
    assert_eq!(
        cache_still_holds(&cache, &content_hash).await,
        Some(true),
        "a partial holder with >=1 verified block must be in the announce set — \
         the old Complete-only `cache.has()` gate would have answered Some(false) here"
    );
    Ok(())
}

/// A front-only partial — block 0 admitted, no tail, so `status()` does not
/// know the size yet — still counts as held. `CacheEngine::coverage` reads
/// the size from the `observe()` bitfield, so the due-time gate keeps a
/// front-to-back fill from its first block.
#[tokio::test]
async fn cache_still_holds_accepts_a_front_only_partial() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let cache = decdn_cache::CacheEngine::open(tmp.path(), vec![], 16).await?;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * group;
    let (root, plaintext, outboard) =
        synth_blob(usize::try_from(total).expect("test blob size fits usize"));
    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard,
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );
    cache.admit_bao(hash, block0_ranges, block0_bao).await?;
    assert!(
        !cache.present_ranges(hash).await?.is_complete(),
        "fixture precondition: the tail was never admitted"
    );

    let content_hash = ContentHash::from_bytes(*hash.as_bytes());
    assert_eq!(cache_still_holds(&cache, &content_hash).await, Some(true));
    Ok(())
}

/// A partial with no whole discovery block is not held for announcing. The
/// bulk seeds schedule such partials without reading their coverage, and
/// this answer is what drops them at their due time.
#[tokio::test]
async fn cache_still_holds_rejects_a_partial_with_no_whole_block() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let cache = decdn_cache::CacheEngine::open(tmp.path(), vec![], 16).await?;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) =
        synth_blob(usize::try_from(total).expect("test blob size fits usize"));
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    cache.admit_bao(hash, ranges, bao).await?;

    let content_hash = ContentHash::from_bytes(*hash.as_bytes());
    assert_eq!(cache_still_holds(&cache, &content_hash).await, Some(false));
    Ok(())
}

/// The cold-start seed and the lag sweep must reach a partial that ranged
/// fills left behind: without it, a node that fills only through ranged
/// pulls seeds nothing at boot (#2186).
#[tokio::test]
async fn holder_snapshot_includes_a_partial_blob() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let cache = decdn_cache::CacheEngine::open(tmp.path(), vec![], 16).await?;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) =
        synth_blob(usize::try_from(total).expect("test blob size fits usize"));
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, 0, 2 * group, total);
    cache.admit_bao(hash, ranges, bao).await?;

    let snap = holder_snapshot(&cache, true).await;

    assert!(snap.hashes.contains(&hash), "{snap:?}");
    Ok(())
}

/// A store fault at the due-time gate answers neither way (#1815): folding
/// it into `Some(false)` would have the tick path `unschedule` the hash —
/// dropping it from the announce set for the process lifetime on a
/// transient store blip.
#[tokio::test]
async fn cache_still_holds_reports_a_store_fault_as_unknown() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let held = decdn_cache::Hash::new(b"held-store-fault");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![foreign_stub_origin(b"held-store-fault")],
        16,
    )
    .await?;
    cache.get(held).await?;
    // Close the store under the gate: the next `has` query faults.
    cache.shutdown().await?;

    let hash = ContentHash::from_bytes(*held.as_bytes());
    assert_eq!(
        cache_still_holds(&cache, &hash).await,
        None,
        "a store fault is not eviction evidence"
    );
    Ok(())
}

/// An empty node yields an empty snapshot rather than an error — the
/// sweep's no-op case.
#[tokio::test]
async fn holder_snapshot_on_an_empty_cache_is_empty() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let cache = decdn_cache::CacheEngine::open(tmp.path(), vec![], 16).await?;

    let snap = holder_snapshot(&cache, true).await;

    assert!(snap.hashes.is_empty(), "{snap:?}");
    assert!(snap.store_error.is_none());
    Ok(())
}

/// A repeated cache event keeps the due time the hash already has: a
/// ranged fill announces once per block it completes, and each must not
/// push the next republish back.
#[test]
fn schedule_steady_if_absent_schedules_a_hash_once() {
    let s = RepublishScheduler::new();
    assert!(s.schedule_steady_if_absent(h(1)), "a new hash is scheduled");
    assert!(
        !s.schedule_steady_if_absent(h(1)),
        "a scheduled hash is left alone"
    );
    assert_eq!(s.heap.lock().map_or(usize::MAX, |h| h.len()), 1);

    let deadline_us = now_us()
        .saturating_add(u64::try_from(STEADY_STATE_MAX.as_micros()).unwrap())
        .saturating_add(1_000);
    let early_us = now_us()
        .saturating_add(u64::try_from(STEADY_STATE_MIN.as_micros()).unwrap())
        .saturating_sub(1_000_000);
    assert!(
        s.drain_due(early_us).is_empty(),
        "the draw is the steady-state window, not the cold-start one"
    );
    assert_eq!(s.drain_due(deadline_us), vec![h(1)]);
    assert!(
        s.schedule_steady_if_absent(h(1)),
        "a drained hash is absent again"
    );
}

/// A seed schedules without announcing, so the eager publish on the hash's
/// first completed block still fires. `unschedule` clears the mark, so a
/// hash that is dropped and later refilled announces again.
#[test]
fn announced_mark_is_independent_of_scheduling() {
    let s = RepublishScheduler::new();
    assert_eq!(s.seed_cold_start([h(1)]), 1);
    assert!(!s.is_announced(&h(1)), "a seed announces nothing");

    s.mark_announced(h(1));
    assert!(s.is_announced(&h(1)));

    s.unschedule(&h(1));
    assert!(!s.is_announced(&h(1)), "unschedule clears the mark");
}

/// A batch counts a hash as announced only where a peer accepted it and its
/// entry advertised at least one block. A rejected entry, or an accepted
/// one with empty coverage, leaves the hash eligible for the eager retry.
#[test]
fn announced_in_batch_keeps_accepted_entries_with_coverage() {
    let covered = Coverage::from_block_indices(1, [0u32].into_iter());
    let entries = vec![
        (h(1), covered.clone()),
        (h(2), covered),
        (h(3), Coverage::empty()),
    ];

    let announced = announced_in_batch(&entries, &[true, false, true]);

    assert_eq!(announced.collect::<Vec<_>>(), vec![h(1)]);
}

/// An ack that answers fewer entries than the batch sent credits none of
/// the unanswered ones.
#[test]
fn announced_in_batch_ignores_entries_the_ack_did_not_answer() {
    let covered = Coverage::from_block_indices(1, [0u32].into_iter());
    let entries = vec![(h(1), covered.clone()), (h(2), covered)];

    let announced = announced_in_batch(&entries, &[true]);

    assert_eq!(announced.collect::<Vec<_>>(), vec![h(1)]);
}

#[test]
fn seed_cold_start_skips_already_scheduled_hashes() {
    // The lag sweep and the periodic origin rescan both seed a
    // scheduler that is already populated. A second entry for an
    // already-scheduled hash would drain twice — once at the earlier
    // due time and again on a later tick, once the tick path has
    // re-added the hash — buying a spurious republish per re-seed.
    let s = RepublishScheduler::new();
    let hashes: Vec<ContentHash> = (1u8..=5).map(h).collect();
    assert_eq!(s.seed_cold_start(hashes.iter().copied()), 5);

    assert_eq!(
        s.seed_cold_start(hashes.iter().copied()),
        0,
        "re-seeding an unchanged held set must schedule nothing new"
    );
    assert_eq!(s.len(), 5, "and must not grow the scheduled set");

    // Assert the heap directly. `len()` / `is_empty()` read the
    // `scheduled` set, which a duplicate entry does not grow, and a
    // single `drain_due` past every due time swallows duplicates via
    // its own `seen` filter — so neither can observe the invariant
    // that actually matters here.
    assert_eq!(
        s.heap.lock().map_or(usize::MAX, |h| h.len()),
        5,
        "a re-seed must push no second heap entry"
    );

    let deadline_us = now_us()
        .saturating_add(u64::try_from(COLD_START_MAX.as_micros()).unwrap())
        .saturating_add(1_000);
    let drained = s.drain_due(deadline_us);
    assert_eq!(drained.len(), 5, "each hash drains once: {drained:?}");
    assert!(s.is_empty());

    // A hash the tick path just drained is absent again, so the next
    // seed re-adds it — "already scheduled" must mean "has a live
    // entry", not "was ever scheduled".
    assert_eq!(
        s.seed_cold_start(hashes.iter().copied()),
        5,
        "a drained hash must be re-seedable"
    );
}

#[test]
fn seed_cold_start_returns_only_newly_scheduled() {
    // Partial overlap is the sweep's real shape: some hashes were
    // committed inside the lag window and are missing, the rest are
    // already scheduled. The count is what the operator log reports,
    // so it must name the repair, not the walk.
    let s = RepublishScheduler::new();
    assert_eq!(s.seed_cold_start((1u8..=3).map(h)), 3);

    assert_eq!(
        s.seed_cold_start((1u8..=5).map(h)),
        2,
        "only the two hashes not already scheduled count"
    );
    assert_eq!(s.len(), 5);
    assert_eq!(
        s.heap.lock().map_or(usize::MAX, |h| h.len()),
        5,
        "the three overlapping hashes must not gain a second heap entry"
    );
}

/// The sweep reports the repair, not the walk: a scheduler that already
/// holds some of the swept hashes counts only what it added. The
/// `reseeded` count is what the operator log and
/// `decdn_dht_republish_sweep_reseeded_total` report, so the distinction
/// is load-bearing.
#[tokio::test]
async fn lag_sweep_counts_only_newly_scheduled_hashes() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let already = decdn_cache::Hash::new(b"sweep-already");
    let missing = decdn_cache::Hash::new(b"sweep-missing");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![
            foreign_stub_origin(b"sweep-already"),
            foreign_stub_origin(b"sweep-missing"),
        ],
        16,
    )
    .await?;
    cache.get(already).await?;
    cache.get(missing).await?;

    let scheduler = RepublishScheduler::new();
    scheduler.schedule_steady(ContentHash::from_bytes(*already.as_bytes()));
    let metrics = crate::metrics::Metrics::new();

    let outcome = lag_sweep(&cache, true, &scheduler, &metrics).await;

    assert_eq!(outcome.reseeded, 1, "only the unscheduled hash is a repair");
    assert!(!outcome.degraded, "a healthy store walk is not degraded");
    assert_eq!(scheduler.len(), 2);
    let text = metrics.encode()?;
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_sweep_reseeded_total 1"),
        "the counter must report the repair, not the walk:\n{text}"
    );
    Ok(())
}

/// A lag that lands while a sweep owns the slot folds into that sweep and
/// bumps the coalesced sibling, so `lag_sweeps_total` stays readable: the
/// difference between the two is the number of lags that claimed an idle
/// slot. Not the number of walks — a folded lag earns the running worker a
/// further pass, which the difference never counts.
///
/// Drives the slot protocol directly with the state pre-claimed. A
/// concurrency-observing variant would have to win a race against a real
/// worker's store walk to assert the same two arms.
#[tokio::test]
async fn a_lag_arriving_mid_sweep_coalesces_and_counts() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let tmp = tempfile::tempdir()?;
    let held = decdn_cache::Hash::new(b"coalesce");
    let cache =
        decdn_cache::CacheEngine::open(tmp.path(), vec![foreign_stub_origin(b"coalesce")], 16)
            .await?;
    // Commit a blob, so "no walk happened" is distinguishable from "a walk
    // happened and found nothing".
    cache.get(held).await?;
    let scheduler = Arc::new(RepublishScheduler::new());
    let metrics = Arc::new(crate::metrics::Metrics::new());
    // Pre-claimed: stand in for a worker mid-walk without racing one.
    let state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_RUNNING));
    let shutdown = CancellationToken::new();
    let slot = SweepSlot {
        state: &state,
        cache: &cache,
        scheduler: &scheduler,
        metrics: &metrics,
        shutdown: &shutdown,
        relay_foreign_namespaces: true,
    };

    // First lag: claims the queue slot behind the running worker.
    spawn_lag_sweep(&slot);
    assert_eq!(state.load(Ordering::Acquire), SWEEP_QUEUED);
    // Second lag: the queued pass already covers it.
    spawn_lag_sweep(&slot);
    assert_eq!(state.load(Ordering::Acquire), SWEEP_QUEUED);

    let text = metrics.encode()?;
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_lag_sweeps_coalesced_total 2"),
        "both folded lags must count, so a slot that never releases is \
         visible as a coalesced rate tracking the lag rate:\n{text}"
    );
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_sweep_reseeded_total 0"),
        "coalescing must not walk the store:\n{text}"
    );
    Ok(())
}

/// A sweep must abandon its walk once shutdown starts.
///
/// The runtime signals the republisher and then flushes and closes the
/// cache store. A walk that keeps going sees the store disappear, takes the
/// store-walk failure arm, and fires an alertable counter plus a
/// degradation warning on an ordinary restart.
#[tokio::test]
async fn a_sweep_abandons_its_walk_once_shutdown_starts() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let tmp = tempfile::tempdir()?;
    let held = decdn_cache::Hash::new(b"shutdown-sweep");
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![foreign_stub_origin(b"shutdown-sweep")],
        16,
    )
    .await?;
    cache.get(held).await?;

    let scheduler = Arc::new(RepublishScheduler::new());
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_IDLE));
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    spawn_lag_sweep(&SweepSlot {
        state: &state,
        cache: &cache,
        scheduler: &scheduler,
        metrics: &metrics,
        shutdown: &shutdown,
        relay_foreign_namespaces: true,
    });

    // The worker is detached; poll for its guard releasing the slot.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if state.load(Ordering::Acquire) == SWEEP_IDLE {
            break;
        }
    }
    assert_eq!(
        state.load(Ordering::Acquire),
        SWEEP_IDLE,
        "the slot guard must release on the cancellation path"
    );
    assert_eq!(
        scheduler.len(),
        0,
        "a cancelled sweep must not have walked the store"
    );
    let text = metrics.encode()?;
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_seed_store_walk_failures_total 0"),
        "a shutdown is not a store-walk degradation:\n{text}"
    );
    Ok(())
}

/// `size` blocks on `gate`, then panics — the injection point for the
/// walk-panic path. Under the origin-only policy the ownership probe is
/// the only origin call a sweep makes, and `size` is that probe; the gate
/// makes the panic's timing deterministic so a test can queue a second
/// pass behind the first before either fires.
#[derive(Debug)]
struct PanickingSizeOrigin {
    data: bytes::Bytes,
    hash: decdn_cache::Hash,
    gate: Arc<tokio::sync::Semaphore>,
}

impl decdn_cache::Origin for PanickingSizeOrigin {
    fn kind(&self) -> decdn_cache::OriginKind {
        decdn_cache::OriginKind::Filesystem
    }

    fn fetch(
        &self,
        hash: decdn_cache::Hash,
        _max_bytes: u64,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>>
                + Send
                + '_,
        >,
    > {
        let result = if hash == self.hash {
            Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
        } else {
            Ok(decdn_cache::OriginFetch::NotFound)
        };
        Box::pin(async move { result })
    }

    fn size(
        &self,
        _hash: decdn_cache::Hash,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<Option<u64>, decdn_cache::OriginPullError>> + Send + '_>,
    > {
        let gate = Arc::clone(&self.gate);
        Box::pin(async move {
            let _permit = gate.acquire().await;
            panic!("synthetic walk panic");
        })
    }
}

/// A panic inside the walk must not discard a queued pass (#1814): the
/// worker observes it as a failed join, meters it, and takes its normal
/// release path — so the queued request still runs, and the slot ends
/// idle rather than stranded.
#[tokio::test]
async fn a_panicking_walk_consumes_the_queued_pass_and_releases_the_slot() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let tmp = tempfile::tempdir()?;
    let payload: &[u8] = b"panic-sweep";
    let held = decdn_cache::Hash::new(payload);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let cache = decdn_cache::CacheEngine::open(
        tmp.path(),
        vec![Arc::new(PanickingSizeOrigin {
            data: bytes::Bytes::from_static(payload),
            hash: held,
            gate: Arc::clone(&gate),
        })],
        16,
    )
    .await?;
    cache.get(held).await?;

    let scheduler = Arc::new(RepublishScheduler::new());
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_IDLE));
    let shutdown = CancellationToken::new();
    let slot = SweepSlot {
        state: &state,
        cache: &cache,
        scheduler: &scheduler,
        metrics: &metrics,
        shutdown: &shutdown,
        // Origin-only, so the walk puts the stored hash to `size` — the
        // gate-then-panic injection point.
        relay_foreign_namespaces: false,
    };

    // First lag claims the slot synchronously; its walk blocks on the
    // gate inside `size`. Second lag queues behind it — deterministic,
    // because the walk cannot finish until the gate opens.
    spawn_lag_sweep(&slot);
    assert_eq!(state.load(Ordering::Acquire), SWEEP_RUNNING);
    spawn_lag_sweep(&slot);
    assert_eq!(state.load(Ordering::Acquire), SWEEP_QUEUED);

    // Open the gate for both passes. The first panics; the worker must
    // consume the queued request and run the second, which panics too —
    // two metered panics prove the queued pass ran rather than being
    // discarded.
    gate.add_permits(2);
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if state.load(Ordering::Acquire) == SWEEP_IDLE {
            break;
        }
    }
    assert_eq!(
        state.load(Ordering::Acquire),
        SWEEP_IDLE,
        "the slot must recover after a panicking walk"
    );
    let text = metrics.encode()?;
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_lag_sweep_panics_total 2"),
        "both passes must run and both panics must count:\n{text}"
    );
    Ok(())
}

#[test]
fn superseded_entry_does_not_drain_twice() {
    // The sweep-vs-eager and sweep-vs-tick races both end in this shape:
    // one hash, two heap entries, the superseded one already overdue. Only
    // the authoritative due time may drain; resurrecting the stale entry
    // buys a spurious republish.
    //
    // The state is built directly rather than through
    // seed-then-reschedule: both due times are drawn from jitter, so the
    // natural path cannot guarantee the stale entry is the overdue one,
    // and a test that only sometimes reaches the check proves nothing.
    let s = RepublishScheduler::new();
    let hash = h(1);
    let now = now_us();
    let stale_due = now.saturating_sub(1_000_000);
    let live_due = now.saturating_add(3_600_000_000);
    {
        let (mut heap, mut scheduled) = (s.heap.lock().unwrap(), s.scheduled.lock().unwrap());
        scheduled.insert(hash, live_due);
        schedule_at(&mut heap, hash, stale_due);
        schedule_at(&mut heap, hash, live_due);
    }

    assert!(
        s.drain_due(now).is_empty(),
        "the superseded entry must not drain"
    );
    assert_eq!(s.len(), 1, "and must not clear the live entry either");
    assert_eq!(
        s.drain_due(live_due),
        vec![hash],
        "the live entry still drains at its own due time"
    );
    assert!(s.is_empty());
}

#[test]
fn unscheduled_tombstone_is_not_resurrected_by_a_later_seed() {
    // Evict-then-re-cache. `unschedule` cannot remove the heap entry, so
    // the tombstone must not drain the freshly scheduled hash ahead of its
    // own due time.
    let s = RepublishScheduler::new();
    let hash = h(2);
    s.seed_cold_start(std::iter::once(hash));
    let tombstone_due = s
        .scheduled
        .lock()
        .unwrap()
        .get(&hash)
        .copied()
        .expect("seeded hash has a due time");
    s.unschedule(&hash);
    assert!(s.is_empty(), "unschedule clears the live entry");

    // Re-cached, scheduled strictly later than the tombstone.
    let fresh_due = tombstone_due.saturating_add(1);
    {
        let (mut heap, mut scheduled) = (s.heap.lock().unwrap(), s.scheduled.lock().unwrap());
        scheduled.insert(hash, fresh_due);
        schedule_at(&mut heap, hash, fresh_due);
    }

    assert!(
        s.drain_due(tombstone_due).is_empty(),
        "the tombstone must not drain the re-scheduled hash early"
    );
    assert_eq!(s.len(), 1);
}

#[test]
fn seed_cold_start_empty_input_is_noop() {
    let s = RepublishScheduler::new();
    let n = s.seed_cold_start(std::iter::empty::<ContentHash>());
    assert_eq!(n, 0);
    assert!(s.is_empty());
}

#[test]
fn steady_state_jitter_in_window() {
    // Drawing 100 steady-state offsets must all land in
    // [STEADY_STATE_MIN, STEADY_STATE_MAX].
    let lo = u64::try_from(STEADY_STATE_MIN.as_micros()).unwrap();
    let hi = u64::try_from(STEADY_STATE_MAX.as_micros()).unwrap();
    for _ in 0..100 {
        let j = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
        assert!((lo..=hi).contains(&j), "offset {j} outside [{lo}, {hi}]");
    }
}

/// A panic under either scheduler guard poisons a sticky `Mutex`. The
/// scheduler must keep scheduling and draining afterwards: an inert
/// scheduler stops advertising every hash the node holds while every
/// liveness signal stays green.
#[test]
fn a_poisoned_lock_does_not_wedge_the_scheduler() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let s = RepublishScheduler::new();
    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        let _heap = s.heap.lock().unwrap();
        let _scheduled = s.scheduled.lock().unwrap();
        panic!("poison both scheduler locks while holding the guards");
    }));
    assert!(poisoned.is_err());
    assert!(s.heap.is_poisoned());
    assert!(s.scheduled.is_poisoned());

    // Schedule in the past so the drain is deterministic.
    s.schedule_with_offset(h(1), 0);
    assert_eq!(s.len(), 1);
    assert!(!s.is_empty());
    assert_eq!(s.drain_due(now_us().saturating_add(1)), vec![h(1)]);
    assert_eq!(s.len(), 0);

    // Unschedule still reaches the map through the poisoned guard.
    s.schedule_with_offset(h(2), 0);
    s.unschedule(&h(2));
    assert!(s.drain_due(now_us().saturating_add(1)).is_empty());
}

#[test]
fn entry_ordering_is_due_us_first() {
    // BinaryHeap with Reverse wrapper should yield earliest due_us
    // first. Verify the underlying Ord directly.
    let a = Entry {
        due_us: 100,
        hash: h(2),
    };
    let b = Entry {
        due_us: 50,
        hash: h(1),
    };
    assert!(b < a);
}
