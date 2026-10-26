use super::*;
use anyhow::Context as _;
use std::sync::Arc;
use std::time::Instant;

/// A node that sheds this client's probes is named as rate-limited, apart
/// from the silent ones; with none, the message keeps its two counts.
#[test]
fn no_serve_target_error_names_rate_limited_nodes_apart_from_silent_ones() {
    let msg = no_serve_target_error(3, 1, 2, 0).to_string();
    assert!(msg.contains("1 did not answer"), "{msg}");
    assert!(msg.contains("2 rate-limited this client's probes"), "{msg}");
    assert!(msg.contains("0 answered but were unusable"), "{msg}");

    let msg = no_serve_target_error(3, 3, 0, 0).to_string();
    assert!(!msg.contains("rate-limited"), "{msg}");
    assert!(msg.contains("3 did not answer, 0 answered"), "{msg}");
}

/// A probe outcome after `ms` on the paused clock: `(id, holder)`.
async fn answer_after(ms: u64, id: u32, holder: bool) -> (u32, bool) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
    (id, holder)
}

/// Every probe that answers within the grace after the first holder is kept.
#[tokio::test(start_paused = true)]
async fn settle_keeps_every_answer_inside_the_grace() {
    let grace = Duration::from_millis(250);
    let probes = vec![
        answer_after(20, 1, true),
        answer_after(150, 2, false),
        answer_after(260, 3, true),
    ];
    let (mut got, tail) = settle_probes(probes, grace, None, |&(_, h)| h, |_| true).await;
    got.sort_unstable();
    assert_eq!(got, vec![(1, true), (2, false), (3, true)]);
    assert!(tail.is_empty());
}

/// A probe still pending at the first holder's answer plus the grace is
/// left in the tail, and the round ends at that moment.
#[tokio::test(start_paused = true)]
async fn settle_leaves_a_straggler_past_the_grace_in_the_tail() {
    let grace = Duration::from_millis(250);
    let started = tokio::time::Instant::now();
    let probes = vec![
        answer_after(25, 1, true),
        answer_after(170, 2, false),
        answer_after(1850, 3, true),
    ];
    let (mut got, tail) = settle_probes(probes, grace, None, |&(_, h)| h, |_| true).await;
    got.sort_unstable();
    assert_eq!(got, vec![(1, true), (2, false)]);
    assert_eq!(tail.len(), 1);
    assert_eq!(started.elapsed(), Duration::from_millis(275));
}

/// With no holder, the round waits for every probe.
#[tokio::test(start_paused = true)]
async fn settle_waits_for_every_probe_without_a_holder() {
    let grace = Duration::from_millis(250);
    let started = tokio::time::Instant::now();
    let probes = vec![answer_after(25, 1, false), answer_after(1850, 2, false)];
    let (got, tail) = settle_probes(probes, grace, None, |&(_, h)| h, |_| true).await;
    assert_eq!(got.len(), 2);
    assert!(tail.is_empty());
    assert_eq!(started.elapsed(), Duration::from_millis(1850));
}

/// A streamed round returns at the first holder; the straggler is in the
/// tail and completes when polled.
#[tokio::test(start_paused = true)]
async fn settle_streams_from_the_first_holder() {
    use futures_util::StreamExt as _;
    let started = tokio::time::Instant::now();
    let probes = vec![
        answer_after(25, 1, true),
        answer_after(170, 2, false),
        answer_after(1850, 3, true),
    ];
    let (got, mut tail) = settle_probes(probes, Duration::ZERO, None, |&(_, h)| h, |_| true).await;
    assert_eq!(got, vec![(1, true)]);
    assert_eq!(started.elapsed(), Duration::from_millis(25));
    let mut late = Vec::new();
    while let Some(outcome) = tail.next().await {
        late.push(outcome);
    }
    assert_eq!(late, vec![(2, false), (3, true)]);
}

/// An answer already in when the first holder answers is collected, not
/// left in the tail.
#[tokio::test(start_paused = true)]
async fn settle_collects_answers_ready_with_the_first_holder() {
    let probes = vec![
        answer_after(25, 1, true),
        answer_after(25, 2, false),
        answer_after(900, 3, true),
    ];
    let (mut got, tail) = settle_probes(probes, Duration::ZERO, None, |&(_, h)| h, |_| true).await;
    got.sort_unstable();
    assert_eq!(got, vec![(1, true), (2, false)]);
    assert_eq!(tail.len(), 1);
}

/// A late probe that records when it answers, then reports `Unreachable`.
fn recording_probe(
    after: Duration,
    answered: &Arc<std::sync::Mutex<Option<Duration>>>,
) -> impl std::future::Future<Output = ProbeOutcome> + Send + 'static {
    let answered = Arc::clone(answered);
    let started = tokio::time::Instant::now();
    async move {
        tokio::time::sleep(after).await;
        if let Ok(mut slot) = answered.lock() {
            *slot = Some(started.elapsed());
        }
        ProbeOutcome::Unreachable
    }
}

fn no_warming() -> ProxyWarmingParams {
    ProxyWarmingParams {
        enabled: false,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

/// A streamed round's pending probes keep running while nothing polls the
/// tail, so a slow unlock between the round and the fetch neither times
/// them out nor inflates their RTT. The answer waits in the tail.
#[tokio::test(start_paused = true)]
async fn late_probes_run_while_the_tail_is_not_polled() {
    use futures_util::StreamExt as _;
    let answered = Arc::new(std::sync::Mutex::new(None));
    let tail: futures_util::stream::FuturesUnordered<_> =
        std::iter::once(recording_probe(Duration::from_millis(100), &answered)).collect();
    let slot = late_slot(ProbeRound::Stream, tail, &[], no_warming());

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(*answered.lock().unwrap(), Some(Duration::from_millis(100)));
    let mut late = slot.take().expect("a streamed round keeps its tail");
    assert!(matches!(
        late.tail.next().await,
        Some(ProbeOutcome::Unreachable)
    ));
    assert!(late.tail.next().await.is_none());
}

/// Dropping the late probes stops the ones still running.
#[tokio::test(start_paused = true)]
async fn late_probes_stop_when_dropped() {
    let answered = Arc::new(std::sync::Mutex::new(None));
    let tail: futures_util::stream::FuturesUnordered<_> =
        std::iter::once(recording_probe(Duration::from_secs(1), &answered)).collect();
    let slot = late_slot(ProbeRound::Stream, tail, &[], no_warming());
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(slot);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(*answered.lock().unwrap(), None);
}

/// Without a cold cutoff, a round with no holder waits for every probe and
/// leaves no tail.
#[tokio::test(start_paused = true)]
async fn settle_without_a_cold_cutoff_waits_for_every_probe() {
    let probes = vec![answer_after(25, 1, false), answer_after(1850, 2, false)];
    let (got, tail) = settle_probes(probes, Duration::ZERO, None, |&(_, h)| h, |_| true).await;
    assert_eq!(got.len(), 2);
    assert!(tail.is_empty());
}

/// With no holder in hand, a cold cutoff ends the round its grace after
/// the first answer; the late holder stays in the tail.
#[tokio::test(start_paused = true)]
async fn settle_cold_cutoff_returns_the_non_holders_in_hand() {
    use futures_util::StreamExt as _;
    let started = tokio::time::Instant::now();
    let probes = vec![
        answer_after(25, 1, false),
        answer_after(100, 2, false),
        answer_after(1850, 3, true),
    ];
    let cold = Some(Duration::from_millis(250));
    let (got, mut tail) = settle_probes(probes, Duration::ZERO, cold, |&(_, h)| h, |_| true).await;
    assert_eq!(got, vec![(1, false), (2, false)]);
    assert_eq!(started.elapsed(), Duration::from_millis(275));
    assert_eq!(tail.next().await, Some((3, true)));
}

/// A holder that answers inside the cold window ends the round at once,
/// as it would with no window.
#[tokio::test(start_paused = true)]
async fn settle_cold_cutoff_yields_to_a_holder() {
    let started = tokio::time::Instant::now();
    let probes = vec![
        answer_after(25, 1, false),
        answer_after(120, 2, true),
        answer_after(1850, 3, false),
    ];
    let cold = Some(Duration::from_millis(250));
    let (got, tail) = settle_probes(probes, Duration::ZERO, cold, |&(_, h)| h, |_| true).await;
    assert_eq!(got, vec![(1, false), (2, true)]);
    assert_eq!(started.elapsed(), Duration::from_millis(120));
    assert_eq!(tail.len(), 1);
}

/// Only a verified answer opens the cold window: a failed probe does not.
#[tokio::test(start_paused = true)]
async fn settle_cold_cutoff_ignores_failed_probes() {
    let started = tokio::time::Instant::now();
    // `(id, holder)`; id 9 stands for a failed probe.
    let probes = vec![answer_after(10, 9, false), answer_after(600, 1, false)];
    let cold = Some(Duration::from_millis(250));
    let (got, tail) = settle_probes(
        probes,
        Duration::ZERO,
        cold,
        |&(_, h)| h,
        |&(id, _)| id != 9,
    )
    .await;
    assert_eq!(got.len(), 2);
    assert!(tail.is_empty());
    assert_eq!(started.elapsed(), Duration::from_millis(600));
}

/// A loopback probe server that closes every connection with `code`, and
/// counts the connections it saw.
async fn closing_probe_server(
    code: u32,
) -> (Endpoint, EndpointAddr, Arc<std::sync::atomic::AtomicUsize>) {
    use iroh::endpoint::{RelayMode, presets};
    let ep = Endpoint::builder(presets::Minimal)
        .alpns(vec![decdn_protocol::ALPN_PROBE.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind_addr(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))
        .unwrap()
        .bind()
        .await
        .unwrap();
    let port = ep
        .bound_sockets()
        .into_iter()
        .find(std::net::SocketAddr::is_ipv4)
        .unwrap()
        .port();
    let addr = EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from((
        std::net::Ipv4Addr::LOCALHOST,
        port,
    )));
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (server, count) = (ep.clone(), Arc::clone(&seen));
    tokio::spawn(async move {
        while let Some(incoming) = server.accept().await {
            let Ok(conn) = incoming.await else { continue };
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            conn.close(code.into(), b"per_peer");
            // Wait for the close to reach the client before the next accept,
            // as the node's probe limiter does.
            conn.closed().await;
        }
    });
    (ep, addr, seen)
}

async fn client_endpoint() -> Endpoint {
    use iroh::endpoint::{RelayMode, presets};
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))
        .unwrap()
        .bind()
        .await
        .unwrap()
}

/// A candidate that sheds every probe with `APP_ERR_RATE_LIMITED` is probed
/// once plus once after each backoff, and its error still reads as a shed —
/// so `probe_and_order` counts it as rate-limited, not silent.
#[tokio::test(flavor = "multi_thread")]
async fn probe_candidate_retries_a_shedding_candidate_then_reports_the_shed() {
    let (server, target, seen) = closing_probe_server(decdn_protocol::APP_ERR_RATE_LIMITED).await;
    let client = client_endpoint().await;
    let started = Instant::now();

    let res = probe_candidate(&client, target, [7; 32], micros_now()).await;

    let err = res.expect_err("every probe was shed");
    assert!(probe_shed(&err), "the shed survives the retries: {err:#}");
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::SeqCst),
        PROBE_SHED_BACKOFFS_MS.len() + 1
    );
    let waited: u64 = PROBE_SHED_BACKOFFS_MS.iter().sum();
    assert!(started.elapsed() >= Duration::from_millis(waited));
    client.close().await;
    server.close().await;
}

/// Any other failure returns at once: a node that closes with a code other
/// than `APP_ERR_RATE_LIMITED` is not probed again.
#[tokio::test(flavor = "multi_thread")]
async fn probe_candidate_does_not_retry_a_failure_that_is_not_a_shed() {
    let (server, target, seen) = closing_probe_server(0).await;
    let client = client_endpoint().await;

    let res = probe_candidate(&client, target, [7; 32], micros_now()).await;

    let err = res.expect_err("the connection was closed");
    assert!(!probe_shed(&err), "{err:#}");
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
    client.close().await;
    server.close().await;
}

/// A node key for the harvest tests. `spawn_harvest` is `pub(crate)`, so
/// they live in-crate rather than in an integration test. They await the
/// returned `JoinHandle`, which the fetch path never does, for determinism.
fn harvest_key(b: u8) -> PublicKey {
    iroh::SecretKey::from_bytes(&[b; 32]).public()
}

fn harvest_candidate(b: u8) -> NodeCandidate {
    NodeCandidate {
        node_id: harvest_key(b),
        eth_address: Address::from([b; 20]),
        region_hint: None,
        multiaddrs: Bytes::new(),
    }
}

#[tokio::test]
async fn harvest_persists_identity_and_stats() {
    let dir = tempfile::tempdir().expect("tempdir");
    let regs = vec![harvest_candidate(1), harvest_candidate(2)];
    let probed = vec![(harvest_key(1), 42.0_f64, 9_u64)];
    let handle = spawn_harvest(dir.path(), regs, probed);
    handle.await.expect("harvest task join");

    let store = crate::PeerStore::open(dir.path());
    assert!(store.get(&harvest_key(2)).is_some());
    let r = store.get(&harvest_key(1)).expect("probed peer persisted");
    assert_eq!(r.latency_ms, Some(42.0));
    assert_eq!(r.rate_per_mb, Some(9));
}

#[test]
fn harvest_counts_every_write_the_store_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A file where the store's directory belongs makes every write fail.
    std::fs::write(dir.path().join("peers"), b"").expect("block the store dir");
    let store = crate::PeerStore::open(dir.path());
    let regs = vec![harvest_candidate(1), harvest_candidate(2)];
    let probed = vec![(harvest_key(1), 42.0_f64, 9_u64)];
    let cfg = crate::StoreConfig::default();
    let tally = harvest(&store, &regs, probed, now_secs(), &cfg);
    assert_eq!(
        tally,
        HarvestTally {
            writes: 3,
            failed: 3
        }
    );
}

/// `select_with_widening` re-runs unfiltered when a region allowlist filters
/// the pool below the freshness floor, so a too-thin allowlist never starves
/// the fetch; an empty allowlist filters nothing.
#[test]
fn select_with_widening_widens_when_allowlist_starves() {
    let cfg = crate::StoreConfig::default();
    let de = decdn_protocol::Region::parse("DE");
    let cands: Vec<NodeCandidate> = (1u8..=4)
        .map(|b| NodeCandidate {
            node_id: harvest_key(b),
            eth_address: Address::from([b; 20]),
            region_hint: de,
            multiaddrs: alloy::primitives::Bytes::new(),
        })
        .collect();
    let us = decdn_protocol::Region::parse("US").expect("US is a valid region");

    // Allowlist excludes every candidate's region -> filtered to zero ->
    // below the floor -> widening re-runs unfiltered and keeps all four.
    let widened = select_with_widening(cands.clone(), None, &[us], &cfg);
    assert_eq!(widened.len(), 4);

    // No allowlist -> nothing filtered, nothing to widen.
    let all = select_with_widening(cands, None, &[], &cfg);
    assert_eq!(all.len(), 4);
}

/// `identity_fresh_candidates` gathers cached peers whose identity is fresh
/// (within the refresh horizon) and not suppressed/prunable, regardless of
/// latency freshness — the set the registry-read-skip path re-probes.
#[test]
fn identity_fresh_candidates_gathers_latency_stale_peers() -> anyhow::Result<()> {
    let cfg = crate::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let now = 1_000_000;

    // Three identity-fresh peers whose latency is STALE (sampled long ago):
    // store_fast_path would reject them, but the registry-skip path keeps them.
    for b in [1u8, 2, 3] {
        store.upsert_identity(&harvest_candidate(b), now)?;
        let stale = now - cfg.latency_ttl_secs - 1;
        store.record_sample(&harvest_key(b), 30.0, 1, stale, &cfg)?;
    }
    // Identity-stale peer (>24h since confirmed) -> excluded.
    store.upsert_identity(&harvest_candidate(4), now - cfg.identity_refresh_secs - 1)?;
    // Freshly-failed peer -> suppressed -> excluded.
    store.upsert_identity(&harvest_candidate(5), now)?;
    store.record_failure(&harvest_key(5), now)?;

    let cands = identity_fresh_candidates(&store, &cfg, now);
    let ids: std::collections::HashSet<_> = cands.iter().map(|c| c.node_id).collect();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&harvest_key(1)));
    assert!(ids.contains(&harvest_key(2)));
    assert!(ids.contains(&harvest_key(3)));
    assert!(!ids.contains(&harvest_key(4)));
    assert!(!ids.contains(&harvest_key(5)));
    Ok(())
}

/// A pinned `--node-id` is one probed-as-holder candidate with no size
/// hint (#2218): its first claim comes from a header-only open.
#[test]
fn a_pinned_node_resolves_with_no_size_hint() {
    let provider = Address::repeat_byte(0x42);
    let targets = super::pinned_targets(harvest_key(7), provider);
    assert!(targets.pinned);
    assert!(targets.size_hint.is_none());
    assert_eq!(targets.candidates.len(), 1);
    assert_eq!(
        targets.candidates.first().map(|c| c.eth_address),
        Some(provider)
    );
}

/// `store_fast_path` ranks selectable records by latency and excludes a
/// suppressed one, and refuses to engage below `min_fresh_candidates`. It
/// probes nothing, so it carries no size hint (#2218).
/// It takes no [`Endpoint`], so it structurally cannot issue a network
/// probe — this is the probe-less fast path itself, not merely tested
/// without one.
#[test]
fn fast_path_needs_min_fresh_and_ranks_by_latency() -> anyhow::Result<()> {
    let cfg = crate::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let now = 10_000;
    for (b, lat) in [(1u8, 80.0), (2, 20.0), (3, 50.0)] {
        store.upsert_identity(&harvest_candidate(b), now)?;
        store.record_sample(&harvest_key(b), lat, 1, now, &cfg)?;
    }
    // A fourth, freshly-failed record: selectable would otherwise admit it,
    // but the failure suppression must exclude it.
    store.upsert_identity(&harvest_candidate(9), now)?;
    store.record_sample(&harvest_key(9), 5.0, 1, now, &cfg)?;
    store.record_failure(&harvest_key(9), now)?;

    let targets =
        store_fast_path(&store, &cfg, 4, now).ok_or_else(|| anyhow::anyhow!("expected Some"))?;
    assert_eq!(targets.candidates.len(), 3);
    assert_eq!(targets.candidates[0].node_id, harvest_key(2)); // lowest latency first
    assert_eq!(targets.candidates[1].node_id, harvest_key(3));
    assert_eq!(targets.candidates[2].node_id, harvest_key(1));
    assert!(targets.coverage_by_node.is_empty());
    assert!(targets.probed_samples.is_empty());
    assert!(
        targets.size_hint.is_none(),
        "nothing was probed, so the first claim comes from a header open"
    );

    // Only two selectable records -> below min_fresh_candidates -> None.
    let dir2 = tempfile::tempdir()?;
    let s2 = crate::PeerStore::open(dir2.path());
    for b in [1u8, 2] {
        s2.upsert_identity(&harvest_candidate(b), now)?;
        s2.record_sample(&harvest_key(b), 30.0, 1, now, &cfg)?;
    }
    assert!(store_fast_path(&s2, &cfg, 4, now).is_none());
    Ok(())
}

/// #2196: nearby nodes serving cold misses from a distant origin open slowly,
/// but a stream open is not a distance sample. After repeated opens on the
/// near nodes, the fast path still ranks them by probe RTT, ahead of the
/// origin.
#[test]
fn stream_opens_do_not_rank_near_nodes_behind_the_origin() -> anyhow::Result<()> {
    let cfg = crate::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let now = now_secs();
    // Probe RTTs: two nearby caches and the transatlantic origin.
    for (b, rtt) in [(1u8, 57.0), (2, 78.0), (3, 394.0)] {
        store.upsert_identity(&harvest_candidate(b), now)?;
        store.record_sample(&harvest_key(b), rtt, 1, now, &cfg)?;
    }
    // A lane's first open files its quoted rate and no latency
    // (`LaneSources::record_open`).
    for _ in 0..5 {
        for b in [1u8, 2] {
            store.record_open(&harvest_key(b), 4)?;
        }
    }

    let near = store
        .get(&harvest_key(1))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert_eq!(near.latency_ms, Some(57.0));
    assert_eq!(near.rate_per_mb, Some(4));
    let targets =
        store_fast_path(&store, &cfg, 4, now).ok_or_else(|| anyhow::anyhow!("expected Some"))?;
    let order: Vec<_> = targets.candidates.iter().map(|c| c.node_id).collect();
    assert_eq!(order, [harvest_key(1), harvest_key(2), harvest_key(3)]);
    Ok(())
}

/// A failed first open stamps the peer, so the fast path skips it, and
/// leaves its probe RTT alone.
#[test]
fn failed_first_open_suppresses_the_peer() -> anyhow::Result<()> {
    let cfg = crate::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let now = now_secs();
    for (b, rtt) in [(1u8, 57.0), (2, 78.0), (3, 90.0), (4, 394.0)] {
        store.upsert_identity(&harvest_candidate(b), now)?;
        store.record_sample(&harvest_key(b), rtt, 1, now, &cfg)?;
    }
    // A lane's delivery fault stamps its peer (`LaneSources::on_source_fault`).
    store.record_failure(&harvest_key(1), now)?;

    let failed = store
        .get(&harvest_key(1))
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    assert!(failed.last_failure_at_secs.is_some());
    assert_eq!(failed.latency_ms, Some(57.0));
    let targets =
        store_fast_path(&store, &cfg, 4, now).ok_or_else(|| anyhow::anyhow!("expected Some"))?;
    let order: Vec<_> = targets.candidates.iter().map(|c| c.node_id).collect();
    assert_eq!(order, [harvest_key(2), harvest_key(3), harvest_key(4)]);
    Ok(())
}

fn node_key(seed: u8) -> PublicKey {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn holder(seed: u8, rtt_ms: f64) -> discovery::Probed {
    super::tests_support::holder(seed, rtt_ms)
}

fn warming_params(enabled: bool) -> ProxyWarmingParams {
    ProxyWarmingParams {
        enabled,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

/// With warming off, the failover order is exactly the holders, nearest RTT
/// first, and no proxy leads.
#[test]
fn failover_order_is_holders_by_rtt_when_warming_off() {
    let holders = vec![holder(3, 300.0), holder(1, 100.0), holder(2, 200.0)];
    let out = super::failover_order(holders, &[], warming_params(false));
    assert!(out.warming_lead.is_none());
    let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
    assert_eq!(ids, vec![node_key(1), node_key(2), node_key(3)]);
}

/// When warming engages, a nearer non-holder leads the list, the rest of the
/// proxies follow nearest first, and the holders form the tail — so a walker
/// gets proxy → … → direct holder (ADR 037 § Fallback).
#[test]
fn failover_order_prepends_proxies_then_holders() {
    // Holders are all distant (>150ms threshold); two proxies beat the best
    // holder (200ms) by ≥30ms, one (190ms) does not.
    let holders = vec![holder(10, 200.0), holder(11, 250.0)];
    let warming_pool = vec![
        discovery::WarmingCandidate {
            node_id: node_key(21),
            eth_address: Address::repeat_byte(21),
            rtt_ms: 90.0,
            multiaddrs: Bytes::new(),
        },
        discovery::WarmingCandidate {
            node_id: node_key(22),
            eth_address: Address::repeat_byte(22),
            rtt_ms: 150.0,
            multiaddrs: Bytes::new(),
        },
        discovery::WarmingCandidate {
            node_id: node_key(23),
            eth_address: Address::repeat_byte(23),
            rtt_ms: 190.0,
            multiaddrs: Bytes::new(),
        },
    ];
    let out = super::failover_order(holders, &warming_pool, warming_params(true));

    // The nearest qualifying proxy (90ms) leads and is reported for the log.
    let lead = out.warming_lead.expect("a proxy should lead");
    assert_eq!(lead.0, node_key(21));
    assert!((lead.2 - 200.0).abs() < f64::EPSILON, "best holder rtt");

    let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
    assert_eq!(
        ids,
        vec![
            node_key(21), // proxy 90ms
            node_key(22), // proxy 150ms (beats 200 by 50 ≥ 30)
            node_key(10), // holder 200ms
            node_key(11), // holder 250ms
        ],
        "proxy 23 (190ms) misses the 30ms margin and is dropped; holders tail the list",
    );
    // The prepended proxy carries no region hint (spoof-proofing, ADR 037).
    assert!(out.order[0].region_hint.is_none());
}

/// Warming that does not engage — no proxy clears the margin — leaves the
/// list as just the holders, with no lead.
#[test]
fn failover_order_no_qualifying_proxy_is_holders_only() {
    let holders = vec![holder(10, 200.0)];
    // A proxy only 10ms nearer misses the 30ms margin.
    let warming_pool = vec![discovery::WarmingCandidate {
        node_id: node_key(21),
        eth_address: Address::repeat_byte(21),
        rtt_ms: 190.0,
        multiaddrs: Bytes::new(),
    }];
    let out = super::failover_order(holders, &warming_pool, warming_params(true));
    assert!(out.warming_lead.is_none());
    let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
    assert_eq!(ids, vec![node_key(10)]);
}

/// With no holder, the failover order is the reachable bonded non-holders,
/// nearest RTT first, as pull-through serve targets (#1911): a cold blob is
/// origin-only with zero cache holders, so an empty holder set is the normal
/// first-fetch state, not a terminal error. The fallback is independent of
/// proxy warming — here warming is off — and reports no lead, size, or
/// holder coverage, since nothing answered `has_blob`.
#[test]
fn failover_order_empty_holders_falls_back_to_non_holders_by_rtt() {
    let non_holders = vec![
        discovery::WarmingCandidate {
            node_id: node_key(31),
            eth_address: Address::repeat_byte(31),
            rtt_ms: 300.0,
            multiaddrs: Bytes::new(),
        },
        discovery::WarmingCandidate {
            node_id: node_key(32),
            eth_address: Address::repeat_byte(32),
            rtt_ms: 100.0,
            multiaddrs: Bytes::new(),
        },
    ];
    let out = super::failover_order(Vec::new(), &non_holders, warming_params(false));

    assert!(out.warming_lead.is_none(), "no holder to warm toward");
    assert!(
        out.coverage_by_node.is_empty(),
        "non-holders carry no measured coverage"
    );
    let ids: Vec<_> = out.order.iter().map(|c| c.node_id).collect();
    assert_eq!(
        ids,
        vec![node_key(32), node_key(31)],
        "nearest non-holder (100ms) leads the pull-through fallback"
    );
    assert!(
        out.order.iter().all(|c| c.region_hint.is_none()),
        "pull-through candidates carry no region hint (spoof-proofing, ADR 037)"
    );
}

/// Registry multiaddrs survive into both rebuilt-candidate branches of
/// `failover_order` — the warming proxies and the cold-order non-holders —
/// so those lanes dial the node directly rather than through a relay.
#[test]
fn failover_order_carries_registry_multiaddrs() -> anyhow::Result<()> {
    let packed = Bytes::from(decdn_incentive::node_register::pack_multiaddrs(&[
        "/ip4/203.0.113.10/udp/4433/quic-v1".to_string(),
    ])?);
    let pool = vec![discovery::WarmingCandidate {
        node_id: node_key(21),
        eth_address: Address::repeat_byte(21),
        rtt_ms: 90.0,
        multiaddrs: packed.clone(),
    }];

    let warm = super::failover_order(vec![holder(10, 200.0)], &pool, warming_params(true));
    let proxy = warm.order.first().context("proxy leads the order")?;
    assert_eq!(proxy.node_id, node_key(21));
    assert_eq!(
        proxy.multiaddrs, packed,
        "warming proxy keeps its addresses"
    );

    let cold = super::failover_order(Vec::new(), &pool, warming_params(false));
    let target = cold
        .order
        .first()
        .context("non-holder is the cold target")?;
    assert_eq!(target.multiaddrs, packed, "cold target keeps its addresses");
    Ok(())
}

/// With neither a holder nor a reachable non-holder, the order is empty —
/// the genuinely terminal case `probe_and_order` turns into an error before
/// it ever calls this.
#[test]
fn failover_order_empty_when_no_holder_and_no_non_holder() {
    let out = super::failover_order(Vec::new(), &[], warming_params(true));
    assert!(out.order.is_empty());
    assert!(out.warming_lead.is_none());
}

/// `failover_order`'s `coverage_by_node` carries each holder's REAL measured
/// coverage (#1506) — not `Coverage::full` for every lane: disjoint
/// per-holder coverage {block0}/{block1} survives into the map keyed by
/// `node_id`, and a node that was never probed (e.g. a proxy) has no entry
/// at all.
#[test]
fn failover_order_coverage_by_node_carries_each_holders_real_coverage() {
    let mut h1 = holder(1, 100.0);
    h1.coverage = decdn_protocol::Coverage::from_block_indices(2, [0].into_iter());
    let mut h2 = holder(2, 200.0);
    h2.coverage = decdn_protocol::Coverage::from_block_indices(2, [1].into_iter());

    let out = super::failover_order(vec![h1.clone(), h2.clone()], &[], warming_params(false));

    assert_eq!(out.coverage_by_node.get(&node_key(1)), Some(&h1.coverage));
    assert_eq!(out.coverage_by_node.get(&node_key(2)), Some(&h2.coverage));
    // Sanity: the two holders' coverage is genuinely different, not both
    // collapsed to the same (e.g. full) value.
    assert_ne!(h1.coverage, h2.coverage);
    // A node that was never probed has no entry.
    assert!(!out.coverage_by_node.contains_key(&node_key(99)));
}

/// #2471: a holder of the empty root answers `has_blob: true` over an empty
/// coverage, because the blob spans no block. The consumer takes that answer
/// as a holder with a size hint of 0 for the empty root, takes a node that
/// signs `has_blob: false` over the same coverage as a non-holder, and drops
/// the `has_blob: true` pairing as malformed for every other hash.
#[test]
fn an_empty_coverage_holder_is_taken_for_the_empty_root_only() -> anyhow::Result<()> {
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::{ProbeSlashData, slash_judge_domain};
    use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};
    use decdn_protocol::{Coverage, EMPTY_ROOT, ProbeResponseExt};

    const TS: u64 = 1_700_000_000_000_000;
    let signer = PrivateKeySigner::random();
    let domain = slash_judge_domain(421_614, alloy::primitives::Address::repeat_byte(0x0D));
    let cand = NodeCandidate {
        node_id: node_key(1),
        eth_address: signer.address(),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let answer = |hash: [u8; 32],
                  has_blob: bool|
     -> anyhow::Result<(ProbeResponse, ProbeResponseExt, crate::probe::ProbeRtt)> {
        let body = ProbeResponseBody {
            hash,
            has_blob,
            rate_per_mb: 10,
            timestamp_us: TS,
        };
        let slash_sig = ProbeSlashData {
            hash: hash.into(),
            has_blob,
            rate_per_mb: 10,
            timestamp_us: TS,
        }
        .sign(&signer, &domain)
        .map_err(|e| anyhow::anyhow!("sign: {e}"))?
        .as_bytes()
        .to_vec();
        Ok((
            ProbeResponse { body, slash_sig },
            ProbeResponseExt {
                total_bytes: Some(0),
                coverage: Coverage::empty(),
            },
            crate::probe::ProbeRtt {
                ms: 5.0,
                direct: true,
            },
        ))
    };

    match classify_probe(&cand, answer(EMPTY_ROOT, true), EMPTY_ROOT, TS, &domain) {
        ProbeOutcome::Holder(probed, _) => {
            assert_eq!(probed.total_bytes, Some(0));
            assert!(probed.coverage.is_empty());
        }
        _ => anyhow::bail!("the empty root's holder must classify as a holder"),
    }
    assert!(
        matches!(
            classify_probe(&cand, answer(EMPTY_ROOT, false), EMPTY_ROOT, TS, &domain),
            ProbeOutcome::NonHolder(..)
        ),
        "a chain-stale or opted-out node signs has_blob:false for the empty root and is a \
         non-holder, not a malformed answer"
    );
    let other_hash = [0x11u8; 32];
    assert!(
        matches!(
            classify_probe(&cand, answer(other_hash, true), other_hash, TS, &domain),
            ProbeOutcome::Unusable
        ),
        "an empty coverage with has_blob:true is malformed for a non-empty root"
    );
    Ok(())
}

/// A candidate's rate hint is its probe sample from this resolve, else the
/// peer store's last rate; a candidate with neither is left out, and the
/// order stays the failover order.
#[test]
fn rate_hints_prefer_the_probe_then_the_store() -> anyhow::Result<()> {
    let (mut targets, node_a, node_b) = tests_support::two_holder_targets();
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let cfg = crate::StoreConfig::default();
    // The store knows b at 40; a's probe sample (1) wins over its store rate.
    store.record_sample(&node_a.node_id, 10.0, 99, now_secs(), &cfg)?;
    store.record_sample(&node_b.node_id, 20.0, 40, now_secs(), &cfg)?;
    targets.probed_samples = vec![(node_a.node_id, 10.0, 1)];
    let hints = targets.rate_hints(&store);
    let got: Vec<_> = hints.iter().map(|h| (h.provider, h.rate_per_mb)).collect();
    assert_eq!(got, vec![(node_a.eth_address, 1), (node_b.eth_address, 40)]);

    // A node neither probed nor recorded gives no hint.
    let empty = tempfile::tempdir()?;
    let hints = targets.rate_hints(&crate::PeerStore::open(empty.path()));
    let got: Vec<_> = hints.iter().map(|h| (h.provider, h.rate_per_mb)).collect();
    assert_eq!(got, vec![(node_a.eth_address, 1)]);
    Ok(())
}
