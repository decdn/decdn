use super::*;
use decdn_incentive::buyer_pool::BuyerLaneProgress;

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

fn common() -> cli::ClientFetchArgs {
    cli::ClientFetchArgs {
        node_id: Some("n".into()),
        rediscover: false,
        addr: None,
        relay_url: None,
        provider_address: Some("0x0000000000000000000000000000000000000001".into()),
        rpc_url: None,
        payment_pool_address: None,
        slash_judge_address: None,
        capacity_bond_address: None,
        region: None,
        proxy_warming: false,
        proxy_warming_rtt_threshold_ms: 150,
        proxy_warming_margin_ms: 30,
        max_sources: 4,
        give_up_after_secs: None,
        chain_id: None,
        keystore: None,
        keystore_password_file: None,
        data_dir: Some(PathBuf::from("/tmp/d")),
        working_deposit_micro_usdc: None,
        max_blob_mb: 1024,
        max_rate_per_mb: 0,
        timeout_ms: 3_600_000,
        capability: None,
        capability_file: None,
    }
}

fn config(body: &str) -> FileConfig {
    toml::from_str(body).expect("parse test config")
}

/// The password file is CLI/env-only — absent unless the operator passes
/// the flag, and carried through verbatim when they do. An absolute path
/// keeps the assertion off the ambient `$HOME` that `expand_tilde` reads.
#[test]
fn keystore_password_file_flows_through_and_defaults_to_none() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\n\
         payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n\
         slash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
    );
    assert!(
        resolve_chain(&common(), &file)
            .unwrap()
            .keystore_password_file
            .is_none()
    );

    let mut a = common();
    a.keystore_password_file = Some(PathBuf::from("/abs/pw.txt"));
    assert_eq!(
        resolve_chain(&a, &file).unwrap().keystore_password_file,
        Some(PathBuf::from("/abs/pw.txt"))
    );
}

/// `-o -` streams to stdout; every other path — including a file literally
/// named `-` addressed as `./-` — writes to disk. Only the exact single-dash
/// output selects the stdout stream (#1848 4b).
#[test]
fn dash_output_selects_the_stdout_stream() {
    assert!(wants_stdout(Path::new("-")));
    assert!(!wants_stdout(Path::new("./-")));
    assert!(!wants_stdout(Path::new("-.bin")));
    assert!(!wants_stdout(Path::new("out.bin")));
}

/// A stdout stream that failed because the fetch gave up ends with the
/// fetch's typed `GaveUp`, not the reader's message, so it exits 75; with
/// no fetch error, the read or write error stands.
#[test]
fn a_stdout_give_up_keeps_its_type() {
    let gave_up = decdn_client::GaveUp {
        idle: Duration::from_mins(10),
    };
    let read_err = anyhow::anyhow!("read verified stream: stream fetch failed: {gave_up}");
    let err = stream_error(read_err, Some(anyhow::Error::new(gave_up)));
    assert_eq!(err.downcast_ref::<decdn_client::GaveUp>(), Some(&gave_up));

    let write_err = stream_error(anyhow::anyhow!("write to stdout: broken pipe"), None);
    assert!(write_err.downcast_ref::<decdn_client::GaveUp>().is_none());
    assert!(write_err.to_string().contains("broken pipe"));
}

/// `copy_verified` drains every byte to the sink in order and reports the
/// final cumulative position as the blob's total — the stdout stream's bar
/// signal (#1848 4b).
#[tokio::test]
async fn copy_verified_streams_all_bytes_and_reports_final_total() -> anyhow::Result<()> {
    let data = vec![7u8; 5000];
    let total = u64::try_from(data.len())?;
    let mut reader: &[u8] = &data;
    let mut sink: Vec<u8> = Vec::new();

    let last = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let observed = Arc::clone(&last);
    let cb = move |pos: u64, _total: u64| {
        observed.store(pos, std::sync::atomic::Ordering::SeqCst);
    };
    let cb_ref: &dyn Fn(u64, u64) = &cb;

    let drained = copy_verified(&mut reader, &mut sink, total, Some(cb_ref)).await?;

    assert_eq!(drained, total, "every byte is drained");
    assert_eq!(sink, data, "the sink receives the bytes in order");
    assert_eq!(
        last.load(std::sync::atomic::Ordering::SeqCst),
        total,
        "the final progress report reaches the blob's total"
    );
    Ok(())
}

/// `spawn_harvest`/`NodeCandidate` are `pub(crate)`, unreachable from an
/// integration test in `crates/cli/tests/` — so this lives in-crate
/// instead of `crates/cli/tests/peer_store_harvest.rs`. The test awaits
/// the returned `JoinHandle` (never done on the real fetch path) purely
/// for determinism.
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

    let store = decdn_client::PeerStore::open(dir.path());
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
    let store = decdn_client::PeerStore::open(dir.path());
    let regs = vec![harvest_candidate(1), harvest_candidate(2)];
    let probed = vec![(harvest_key(1), 42.0_f64, 9_u64)];
    let cfg = decdn_client::StoreConfig::default();
    let tally = harvest(&store, &regs, probed, now_secs_cli(), &cfg);
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
    let cfg = decdn_client::StoreConfig::default();
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

/// `cached_pool_token` returns the persisted pool's immutable token without
/// any contract read, and `None` (so the caller reads `usdc()` on-chain) when
/// no pool row exists for the owner yet.
#[test]
fn cached_pool_token_uses_persisted_row_and_skips_rpc() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    // A fresh subpath so the store's `ensure_data_dir` creates it at 0o700;
    // the tempdir root itself is 0o755, which the store rejects.
    let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
    let owner = Address::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let deployment = Deployment {
        chain_id: 421_614,
        payment_pool: Address::repeat_byte(0x9c),
    };

    // No row yet -> None -> caller must read usdc() on the open path.
    assert_eq!(cached_pool_token(&store, owner, deployment)?, None);

    // Persist a pool row for this owner.
    let state = BuyerPoolState::new(
        B256::repeat_byte(0xAB),
        deployment,
        owner,
        token,
        U256::from(1_000u64),
    );
    store.record(&state)?;

    // Row present -> the immutable token comes back with no contract read.
    assert_eq!(cached_pool_token(&store, owner, deployment)?, Some(token));
    // A different owner has no row -> still None.
    assert_eq!(
        cached_pool_token(&store, Address::repeat_byte(0x33), deployment)?,
        None
    );
    // The row is the same, but it names another deployment's `usdc()`.
    // Answering with it would approve and price the wrong token. The
    // address alone does not name a deployment: the same address on
    // another chain is another contract.
    let other_address = Deployment {
        payment_pool: Address::repeat_byte(0xDE),
        ..deployment
    };
    let other_chain = Deployment {
        chain_id: 1,
        ..deployment
    };
    for foreign in [other_address, other_chain] {
        assert_eq!(
            cached_pool_token(&store, owner, foreign)?,
            None,
            "a row from another PaymentPool deployment must not seed the token cache \
             ({foreign:?})"
        );
    }
    Ok(())
}

/// `identity_fresh_candidates` gathers cached peers whose identity is fresh
/// (within the refresh horizon) and not suppressed/prunable, regardless of
/// latency freshness — the set the registry-read-skip path re-probes.
#[test]
fn identity_fresh_candidates_gathers_latency_stale_peers() -> anyhow::Result<()> {
    let cfg = decdn_client::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = decdn_client::PeerStore::open(dir.path());
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
    let cfg = decdn_client::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = decdn_client::PeerStore::open(dir.path());
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
    let s2 = decdn_client::PeerStore::open(dir2.path());
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
    let cfg = decdn_client::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = decdn_client::PeerStore::open(dir.path());
    let now = now_secs_cli();
    // Probe RTTs: two nearby caches and the transatlantic origin.
    for (b, rtt) in [(1u8, 57.0), (2, 78.0), (3, 394.0)] {
        store.upsert_identity(&harvest_candidate(b), now)?;
        store.record_sample(&harvest_key(b), rtt, 1, now, &cfg)?;
    }
    // A lane's first open files its quoted rate and no latency
    // (`CliSources::record_open`).
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
    let cfg = decdn_client::StoreConfig::default();
    let dir = tempfile::tempdir()?;
    let store = decdn_client::PeerStore::open(dir.path());
    let now = now_secs_cli();
    for (b, rtt) in [(1u8, 57.0), (2, 78.0), (3, 90.0), (4, 394.0)] {
        store.upsert_identity(&harvest_candidate(b), now)?;
        store.record_sample(&harvest_key(b), rtt, 1, now, &cfg)?;
    }
    // A lane's delivery fault stamps its peer (`CliSources::on_source_fault`).
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

fn ctx_with(binding: Option<decdn_protocol::client::ClientBinding>) -> PoolContext {
    ctx_with_deposit(binding, U256::ZERO)
}

fn ctx_with_deposit(
    binding: Option<decdn_protocol::client::ClientBinding>,
    deposit: U256,
) -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        provider: Address::ZERO,
        deposit,
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: bind_node_id_domain(1, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: binding,
        capability: None,
    }
}

/// A registry-backed `lane_ledger` call for the same lane must return the
/// SAME ledger `Arc` on a second call (no fresh mint per fetch/chunk) — the
/// invariant `LaneLedgers::get_or_insert` exists to guarantee. A `None`
/// registry (standalone `decdn fetch`) must mint a distinct ledger every
/// call.
#[test]
fn lane_ledger_shares_one_arc_through_the_registry_but_not_without_one() {
    let lane = LaneKey {
        pool_id: B256::ZERO,
        signer: Address::ZERO,
        provider: Address::from([7u8; 20]),
    };
    let registry = LaneLedgers::new();
    let (ledger_a, ctx_a) = lane_ledger(Some(&registry), lane, ctx_with(None));
    let (ledger_b, ctx_b) = lane_ledger(Some(&registry), lane, ctx_with(None));
    assert!(Arc::ptr_eq(&ledger_a, &ledger_b));
    assert!(Arc::ptr_eq(&ctx_a, &ctx_b));

    let (ledger_none_a, _) = lane_ledger(None, lane, ctx_with(None));
    let (ledger_none_b, _) = lane_ledger(None, lane, ctx_with(None));
    assert!(!Arc::ptr_eq(&ledger_none_a, &ledger_none_b));
}

/// A second `lane_ledger` touch of an already-registered lane must reconcile
/// the shared handle's `ctx.deposit` UPWARD to a freshly-read higher deposit
/// (a low-water top-up `open_or_reuse_pool` performed between the two
/// touches) rather than silently discarding it. The pool-wide spent/credit
/// gate reads this shared value, so a stale low deposit would refuse
/// (`PoolExhausted`) prematurely even though the on-chain balance grew.
#[test]
fn lane_ledger_reconciles_shared_deposit_upward_on_reuse() {
    let lane = LaneKey {
        pool_id: B256::ZERO,
        signer: Address::ZERO,
        provider: Address::from([9u8; 20]),
    };
    let registry = LaneLedgers::new();
    let (_, ctx_a) = lane_ledger(
        Some(&registry),
        lane,
        ctx_with_deposit(None, U256::from(100)),
    );
    assert_eq!(
        ctx_a.lock().unwrap_or_else(PoisonError::into_inner).deposit,
        U256::from(100)
    );

    // A later touch on the same lane with a higher freshly-read deposit
    // (simulating a top-up) must raise the shared handle, not discard it.
    let (_, ctx_b) = lane_ledger(
        Some(&registry),
        lane,
        ctx_with_deposit(None, U256::from(250)),
    );
    assert!(Arc::ptr_eq(&ctx_a, &ctx_b));
    assert_eq!(
        ctx_b.lock().unwrap_or_else(PoisonError::into_inner).deposit,
        U256::from(250)
    );

    // A lower freshly-read deposit than the shared handle's current value
    // must never lower it (deposit only ever grows via top-ups).
    let (_, ctx_c) = lane_ledger(
        Some(&registry),
        lane,
        ctx_with_deposit(None, U256::from(10)),
    );
    assert_eq!(
        ctx_c.lock().unwrap_or_else(PoisonError::into_inner).deposit,
        U256::from(250)
    );
}

/// The refusal these tests annotate, built the way the fetch path builds it: the typed
/// `UpstreamRefused` sentinel (#1144). Never hand-roll one with
/// `anyhow!("delivery refused: …")` — the annotation downcasts, so a look-alike string
/// would exercise nothing and pass against a hint that never fires in production.
fn refusal(error: StreamError) -> anyhow::Error {
    anyhow::Error::new(UpstreamRefused::mid_stream(error))
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

/// The delegated signer gate accepts the authorized key and rejects any
/// other, naming both addresses so the operator can see the mismatch.
#[test]
fn ensure_delegate_signer_matches_or_rejects() {
    let key = Address::repeat_byte(0xa1);
    assert!(super::ensure_delegate_signer(key, key).is_ok());

    let other = Address::repeat_byte(0xb2);
    let err = super::ensure_delegate_signer(other, key)
        .expect_err("a non-authorized key must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains(&key.to_string()),
        "names the authorized signer: {msg}"
    );
    assert!(
        msg.contains(&other.to_string()),
        "names the loaded key: {msg}"
    );
}

/// A delegated `SpendingCapExhausted` is reconnected to the owner-side remedy; the
/// delegate holds no wallet on the pool, so "top up / re-issue" is the fix.
#[test]
fn delegated_spending_cap_exhausted_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    });
    let annotated = super::annotate_delegated_exhaustion(err);
    assert!(
        annotated.to_string().contains("exhausted"),
        "expected the exhaustion remedy, got: {annotated}"
    );
}

/// A drained signer registration names the remedy a write-once
/// registration leaves: a capability for a new signer key (#2338). Only a
/// registration expired or with nothing left of its cap is named as
/// shutting out every provider. A drain read at one provider's rate (alone,
/// or under the stop once every known provider is barred) and a mid-stream
/// `SignerCapExhausted` are measured against the refusing provider's rate,
/// so the text says a cheaper provider may still serve.
#[test]
fn delegated_drained_signer_gets_the_new_key_remedy() {
    let drained = |remaining, expired| decdn_client::SignerCapDrained {
        pool_id: alloy::primitives::B256::ZERO,
        signer: Address::repeat_byte(0xd1),
        provider: Address::repeat_byte(0xa1),
        remaining,
        rate_per_mb: 10,
        expired,
    };
    let at_rate = [
        anyhow::Error::new(drained(5, false)),
        anyhow::Error::new(drained(5, false)).context(decdn_client::NoSourceServesSigner),
        anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::SignerCapExhausted,
            bundle: None,
            proof_generation: None,
        }),
    ];
    for err in at_rate {
        let annotated = format!("{:#}", super::annotate_delegated_exhaustion(err));
        assert!(
            annotated.contains("new signer key")
                && annotated.contains("lower rate may still serve")
                && !annotated.contains("no node can be paid"),
            "expected the rate-relative new-key remedy, got: {annotated}"
        );
    }
    for err in [
        anyhow::Error::new(drained(0, false)),
        anyhow::Error::new(drained(5, true)),
    ] {
        let annotated = format!("{:#}", super::annotate_delegated_exhaustion(err));
        assert!(
            annotated.contains("new signer key") && annotated.contains("no node can be paid"),
            "expected the every-provider new-key remedy, got: {annotated}"
        );
    }
}

/// Any error the delegated-exhaustion annotator does not name passes
/// through untouched.
#[test]
fn delegated_non_cap_error_is_untouched() {
    let annotated = super::annotate_delegated_exhaustion(anyhow::anyhow!("stalled"));
    assert_eq!(annotated.to_string(), "stalled");
}

/// A delegated `CapabilityExpired` rejection also gets the owner-remedy
/// hint: the delegate cannot mint itself a fresh capability either.
#[test]
fn delegated_capability_expired_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::CapabilityExpired,
        bundle: None,
        proof_generation: None,
    });
    let annotated = super::annotate_delegated_exhaustion(err);
    assert!(
        annotated.to_string().contains("expired"),
        "expected the expiry remedy, got: {annotated}"
    );
}

/// A delegated `PoolExhausted` rejection also gets the owner-remedy
/// hint: it is a pool-wide deposit shortfall, not this signer's cap.
#[test]
fn delegated_pool_exhausted_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::PoolExhausted,
        bundle: None,
        proof_generation: None,
    });
    let annotated = super::annotate_delegated_exhaustion(err);
    assert!(
        annotated.to_string().contains("exhausted"),
        "expected the exhaustion remedy, got: {annotated}"
    );
}

/// An unbound (no `capacity_bond_address`) fetch refused with `NotFound` gets
/// the actionable hint attached, reconnecting the opaque refusal to its cause.
#[test]
fn unbound_notfound_refusal_gets_actionable_hint() {
    let annotated = annotate_unbound_cache_miss(refusal(StreamError::NotFound), &ctx_with(None));
    assert!(
        annotated.to_string().contains("capacity_bond_address"),
        "expected the binding hint, got: {annotated}"
    );
}

/// Every source saying `NotFound` ends the fetch as `NoSourceHasBlob`, with
/// no `UpstreamRefused` left in the chain: an unbound fetch still gets the
/// binding hint, and a bound one passes through untouched.
#[test]
fn unbound_no_source_has_blob_gets_the_binding_hint() {
    let absent = || anyhow::Error::new(NoSourceHasBlob);
    let annotated = annotate_unbound_cache_miss(absent(), &ctx_with(None));
    assert!(
        annotated.to_string().contains("capacity_bond_address"),
        "expected the binding hint, got: {annotated}"
    );
    assert!(annotated.downcast_ref::<NoSourceHasBlob>().is_some());

    let signer = PrivateKeySigner::random();
    let binding = sign_client_binding(&signer, B256::ZERO, &bind_node_id_domain(1, Address::ZERO))
        .expect("sign binding");
    let bound = annotate_unbound_cache_miss(absent(), &ctx_with(Some(binding)));
    assert_eq!(bound.to_string(), NoSourceHasBlob.to_string());
}

/// A delegated fetch no provider's voucher fits names the owner-side remedy.
#[test]
fn delegated_no_affordable_source_gets_the_owner_remedy_hint() {
    let err = anyhow::Error::new(NoAffordableSource {
        deposit: U256::from(5u32),
    });
    let annotated = annotate_delegated_exhaustion(err);
    assert!(
        annotated
            .to_string()
            .contains("Ask the pool owner to top up"),
        "{annotated}"
    );
}

/// A bound fetch's error is passed through untouched — a `NotFound` there is a
/// genuine miss, not a missing-binding problem.
#[test]
fn bound_notfound_refusal_is_untouched() {
    let signer = PrivateKeySigner::random();
    let binding = sign_client_binding(&signer, B256::ZERO, &bind_node_id_domain(1, Address::ZERO))
        .expect("sign binding");
    let annotated =
        annotate_unbound_cache_miss(refusal(StreamError::NotFound), &ctx_with(Some(binding)));
    assert!(!annotated.to_string().contains("capacity_bond_address"));
}

/// A refusal that is NOT `NotFound` gets no binding hint even when unbound: a
/// client binding authorizes reactive pull-through, so it cannot fix a node
/// that is degraded (`InternalError`) or a blob that is over the ceiling.
#[test]
fn unbound_non_notfound_refusal_is_untouched() {
    for error in [StreamError::InternalError, StreamError::BlobTooLarge] {
        let annotated = annotate_unbound_cache_miss(refusal(error.clone()), &ctx_with(None));
        assert!(
            !annotated.to_string().contains("capacity_bond_address"),
            "{error:?} must not get the binding hint"
        );
    }
}

/// A non-`NotFound` failure (e.g. a transport error) is never mislabeled as a
/// missing-binding problem, even when unbound.
#[test]
fn unbound_non_notfound_error_is_untouched() {
    let annotated = annotate_unbound_cache_miss(
        anyhow::anyhow!("connect failed: timed out"),
        &ctx_with(None),
    );
    assert!(!annotated.to_string().contains("capacity_bond_address"));
}

#[test]
fn flags_override_config() {
    let mut c = common();
    c.rpc_url = Some("http://flag:8545".into());
    c.chain_id = Some(99);
    let pp = "0x1111111111111111111111111111111111111111";
    c.payment_pool_address = Some(pp.into());
    c.slash_judge_address = Some("0x2222222222222222222222222222222222222222".into());
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
    );
    let r = resolve_chain(&c, &file).unwrap();
    assert_eq!(r.rpc_url, "http://flag:8545");
    assert_eq!(r.chain_id, 99);
    assert_eq!(r.payment_pool, Address::from_str(pp).unwrap());
}

#[test]
fn config_fills_unset_flags_and_defaults() {
    let file = config(
        "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\nbuyer_working_deposit_micro_usdc = 5000000\n",
    );
    let r = resolve_chain(&common(), &file).unwrap();
    assert_eq!(r.rpc_url, "http://config:8545");
    // chain_id absent everywhere → default.
    assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
    assert_eq!(r.working_deposit, U256::from(5_000_000u64));
    // keystore defaults under the data dir.
    assert_eq!(
        r.keystore,
        eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
    );
}

/// `resolve_chain` must enforce the same deposit invariant the daemon
/// resolver does. It reads the raw `[blockchain]` table plus the
/// CLI flags rather than going through `resolve_blockchain_into`, so without
/// its own check `decdn fetch` would accept a config file that
/// `decdn config validate` rejects — a validator that does not validate what
/// actually runs — and `--working-deposit-micro-usdc 0` would reach the chain
/// and surface as an opaque `openPool` `ZeroAmount` revert.
#[test]
fn resolve_chain_rejects_deposits_the_daemon_resolver_would_reject() {
    let base = "[blockchain]\nrpc_url = \"http://config:8545\"\npayment_pool_address = \"0x3333333333333333333333333333333333333333\"\nslash_judge_address = \"0x4444444444444444444444444444444444444444\"\n";

    // A zero working deposit can never open a pool.
    let err = resolve_chain(
        &common(),
        &config(&format!("{base}buyer_working_deposit_micro_usdc = 0\n")),
    )
    .expect_err("a zero working deposit must be refused at resolve time");
    assert!(
        err.to_string().contains("buyer_working_deposit_micro_usdc"),
        "the error must name the offending field; got: {err}"
    );
}

fn rebase_store_fixture() -> anyhow::Result<(
    tempfile::TempDir,
    RedbBuyerPoolStore,
    Address,
    PoolId,
    LaneKey,
)> {
    let dir = tempfile::tempdir()?;
    // A fresh subpath so the store's `ensure_data_dir` creates it at 0o700.
    let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
    let owner = Address::repeat_byte(0x0A);
    let pool_id = PoolId::repeat_byte(0x01);
    let lane = LaneKey {
        pool_id,
        signer: owner,
        provider: Address::repeat_byte(0xB0),
    };
    let mut state = BuyerPoolState::new(
        pool_id,
        Deployment {
            chain_id: 421_614,
            payment_pool: Address::repeat_byte(0x7B),
        },
        owner,
        Address::repeat_byte(0x7C),
        U256::from(1_000u64),
    );
    state
        .advance_lane(lane, U256::from(500u64), U256::from(90u64))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    store.record(&state)?;
    Ok((dir, store, owner, pool_id, lane))
}

fn lane_record(
    store: &RedbBuyerPoolStore,
    pool_id: PoolId,
    lane: LaneKey,
) -> anyhow::Result<BuyerLaneProgress> {
    store
        .get_by_pool_id(pool_id)?
        .and_then(|s| s.lane_progress(lane))
        .ok_or_else(|| anyhow::anyhow!("lane record missing"))
}

/// Two bundle entries on one provider share one connection: each entry
/// builds its own deps and lane, and both lanes open their streams on the
/// command's connection to the node.
#[allow(
    clippy::too_many_lines,
    reason = "one fixture: a stream-dropping node, the lane deps, two entries"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_entries_on_one_provider_share_one_connection() -> anyhow::Result<()> {
    use decdn_client::source::BlobSource as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    async fn loopback(alpns: Vec<Vec<u8>>) -> anyhow::Result<(Endpoint, EndpointAddr)> {
        let ep = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(alpns)
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))?
            .bind()
            .await?;
        let socket = ep
            .bound_sockets()
            .into_iter()
            .find(std::net::SocketAddr::is_ipv4)
            .ok_or_else(|| anyhow::anyhow!("no IPv4 socket"))?;
        let addr = EndpointAddr::new(ep.id()).with_ip_addr(socket);
        Ok((ep, addr))
    }

    // A node that accepts every connection and drops every stream it is
    // sent, so each open fails on a connection that stays up.
    let (node, node_addr) = loopback(vec![decdn_protocol::ALPN_CLIENT.to_vec()]).await?;
    let accepted = Arc::new(AtomicU64::new(0));
    let (accept_node, count) = (node.clone(), Arc::clone(&accepted));
    tokio::spawn(async move {
        while let Some(incoming) = accept_node.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                while let Ok((send, recv)) = conn.accept_bi().await {
                    drop((send, recv));
                }
            });
        }
    });

    let (ep, _) = loopback(Vec::new()).await?;
    let dir = tempfile::tempdir()?;
    let store = Arc::new(RedbBuyerPoolStore::open(&dir.path().join("data"))?);
    let rpc = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new());
    let contract = PaymentPool::new(Address::repeat_byte(0x33), rpc.clone());
    let chain = resolve_chain(
        &common(),
        &config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n\
             slash_judge_address = \"0x4444444444444444444444444444444444444444\"\n",
        ),
    )?;
    let slash_dom = decdn_incentive::slash_judge_domain(1, Address::repeat_byte(0x44));
    let connections = Connections::new(ep.clone());
    let writes = OrderedWrites::default();
    let funding = RunFunding::default();
    let entry_deps = || -> anyhow::Result<DriveFetchDeps<'_, _>> {
        Ok(DriveFetchDeps {
            timings: None,
            endpoint: &ep,
            store: &store,
            contract: &contract,
            rpc: &rpc,
            slash_dom: &slash_dom,
            self_address: Address::repeat_byte(0x0A),
            token: Address::repeat_byte(0x22),
            chain: &chain,
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            max_rate_per_mb: 0,
            max_blob_bytes: 0,
            deadlines: PullDeadlines::new(Duration::from_secs(5), Duration::from_secs(5), 0)?,
            connections: &connections,
            writes: &writes,
            funding: &funding,
        })
    };
    let provider = Address::repeat_byte(0xB0);
    let ctx = PoolContext {
        pool_id: B256::ZERO,
        provider,
        deposit: U256::from(1_000_000u64),
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33)),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    };
    let ledger = Arc::new(ctx.new_ledger());
    let ctx = Arc::new(Mutex::new(ctx));
    let range = decdn_bao_range::align_range(0, 0, 1024)?;
    for entry in [[0x01u8; 32], [0x02u8; 32]] {
        let deps = entry_deps()?;
        let source = lane_source(
            &deps,
            node_addr.clone(),
            Arc::clone(&ctx),
            Arc::clone(&ledger),
            provider,
        );
        assert!(
            source.open(entry, range.clone()).await.is_err(),
            "the node drops every stream"
        );
    }

    assert_eq!(connections.dials(), 1, "both entries share one dial");
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn settle_on_drop_runs_only_when_dropped_armed() {
    let settled = std::cell::Cell::new(0);
    drop(SettleOnDrop::new(|| settled.set(settled.get() + 1)));
    assert_eq!(settled.get(), 1, "a dropped drive settles");
    SettleOnDrop::new(|| settled.set(settled.get() + 1)).disarm();
    assert_eq!(settled.get(), 1, "a returned drive settles on its own path");
}

/// `persist_watermark` overwrites the lane record down to a rebase anchor and
/// advances it to the ledger's totals, even below what the record holds. A
/// persist without an anchor is a monotone advance again, so a lower totals
/// snapshot cannot move the record down.
#[test]
fn persist_watermark_overwrites_once_on_a_rebase() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let anchor = Cumulative {
        bytes: U256::from(100u64),
        amount: U256::from(60u64),
    };
    let totals = Cumulative {
        bytes: U256::from(200u64),
        amount: U256::from(70u64),
    };
    let progress = VoucherProgress::from_cumulative(totals, U256::from(90u64))
        .with_rebase_anchor(Some(anchor));
    persist_watermark(&store, owner, pool_id, lane, &progress);
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (totals.bytes, totals.amount)
    );

    let lower = Cumulative {
        bytes: U256::from(150u64),
        amount: U256::from(65u64),
    };
    let progress = VoucherProgress::from_cumulative(lower, U256::ZERO);
    persist_watermark(&store, owner, pool_id, lane, &progress);
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (totals.bytes, totals.amount),
        "without an anchor the persist is monotone"
    );
    Ok(())
}

/// A lane built twice on one shared ledger commits once per entry; a lane
/// on its own ledger keeps its own write. The one write takes the lowest
/// baseline of the ledger's handles: a sibling's rebase moved the store
/// from 90 to 70 between the two builds, and the ledger settled at 80, so
/// the advance from 70 to 80 still persists.
#[test]
fn duplicate_lane_handles_commit_once_from_the_lowest_baseline() {
    let handle = |ledger: &Arc<PoolLedger>, provider: u8, prior: u64| FaceLaneHandle {
        pool_id: PoolId::repeat_byte(1),
        provider: Address::repeat_byte(provider),
        prior_amount: U256::from(prior),
        ledger: Arc::clone(ledger),
        ctx: Arc::new(Mutex::new(ctx_with(None))),
    };
    let shared = Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(100u64),
        amount: U256::from(80u64),
    }));
    let other = Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(10u64),
        amount: U256::from(7u64),
    }));
    let handles = [
        handle(&shared, 0xA1, 90),
        handle(&other, 0xB2, 0),
        handle(&shared, 0xA1, 70),
    ];
    let writes = face_watermark_writes(Address::repeat_byte(0x5E), &handles);
    let providers: Vec<Address> = writes.iter().map(|(lane, _)| lane.provider).collect();
    assert_eq!(
        providers,
        [Address::repeat_byte(0xA1), Address::repeat_byte(0xB2)]
    );
    assert_eq!(
        writes[0].1.advanced(),
        Some((U256::from(100u64), U256::from(80u64))),
        "the shared lane advances past its lowest baseline"
    );
}

/// Hold `writes` until the returned sender fires, so every write queued
/// meanwhile waits behind the hold.
fn hold(writes: &OrderedWrites) -> std::sync::mpsc::Sender<()> {
    let (release, hold) = std::sync::mpsc::channel::<()>();
    writes.queue(move || {
        let _ = hold.recv();
    });
    release
}

/// One lane's write of `totals` over baseline `prior`, rebased to `anchor`.
fn lane_write(
    lane: LaneKey,
    totals: (u64, u64),
    prior: u64,
    anchor: Option<(u64, u64)>,
) -> LaneWrite {
    let cum = |(bytes, amount): (u64, u64)| Cumulative {
        bytes: U256::from(bytes),
        amount: U256::from(amount),
    };
    (
        lane,
        VoucherProgress::from_cumulative(cum(totals), U256::from(prior))
            .with_rebase_anchor(anchor.map(cum)),
    )
}

/// Two entries settle one lane: the first read takes the ledger's rebase
/// (down to 60, totals 70), the second a newer advance to 80. They land
/// in that order, so the rebase never replaces the newer row.
#[tokio::test]
async fn a_rebase_read_first_never_lands_over_a_newer_advance() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let store = Arc::new(store);
    let writes = OrderedWrites::default();
    let release = hold(&writes);
    let rebase = lane_write(lane, (200, 70), 90, Some((100, 60)));
    drop(queue_watermark_writes(&writes, &store, owner, vec![rebase]));
    let advance = lane_write(lane, (300, 80), 70, None);
    let landed = queue_watermark_writes(&writes, &store, owner, vec![advance]);
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(10), landed).await??;
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (U256::from(300u64), U256::from(80u64))
    );
    Ok(())
}

/// Two entries settle one lane: the first read is an advance to 95 from
/// before the node refused it, the second the rebase down to 60 (totals
/// 70). They land in that order, so the refused watermark never returns.
#[tokio::test]
async fn a_stale_advance_read_first_never_lands_over_a_rebase() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let store = Arc::new(store);
    let writes = OrderedWrites::default();
    let release = hold(&writes);
    let advance = lane_write(lane, (600, 95), 90, None);
    drop(queue_watermark_writes(
        &writes,
        &store,
        owner,
        vec![advance],
    ));
    let rebase = lane_write(lane, (200, 70), 90, Some((100, 60)));
    let landed = queue_watermark_writes(&writes, &store, owner, vec![rebase]);
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(10), landed).await??;
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (U256::from(200u64), U256::from(70u64))
    );
    Ok(())
}

/// A drop guard's settle drops its receiver and returns. The write it
/// queued, behind a slow one, still lands before the runtime finishes
/// dropping: the runtime waits for the blocking pool as it shuts down.
#[test]
fn a_detached_settle_lands_by_runtime_shutdown() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let store = Arc::new(store);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()?;
    runtime.block_on(async {
        let writes = OrderedWrites::default();
        writes.queue(|| std::thread::sleep(Duration::from_millis(100)));
        let advance = lane_write(lane, (600, 95), 90, None);
        drop(queue_watermark_writes(
            &writes,
            &store,
            owner,
            vec![advance],
        ));
    });
    drop(runtime);
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (U256::from(600u64), U256::from(95u64))
    );
    Ok(())
}

/// The multi-source path carries each lane's own rebase anchor.
#[test]
fn multi_lane_watermarks_carry_the_rebase_anchor() {
    let anchor = Cumulative {
        bytes: U256::from(50u64),
        amount: U256::from(60u64),
    };
    let mut lane = lane_wm(1, 0xA1, 200, 100, 70);
    lane.rebase_anchor = Some(anchor);
    let out = super::multi_lane_watermarks(Address::repeat_byte(0x5E), &[lane]);
    assert_eq!(out[0].1.rebase_anchor(), Some(anchor));
    assert_eq!(out[0].1.totals(), (U256::from(100u64), U256::from(70u64)));
}

// ---- per-lane watermarks on the multi-source path ----

fn lane_wm(pool: u8, provider: u8, prior: u64, bytes: u64, amount: u64) -> LaneWatermark {
    LaneWatermark {
        pool_id: PoolId::repeat_byte(pool),
        provider: Address::repeat_byte(provider),
        prior_amount: U256::from(prior),
        settlement: Cumulative {
            bytes: U256::from(bytes),
            amount: U256::from(amount),
        },
        rebase_anchor: None,
    }
}

/// Each lane's watermark is persisted under ITS OWN `LaneKey` and carries ITS
/// OWN cumulative. Crossing the two — lane A's amount under lane B's key —
/// strands both channels, and nothing else in the fetch path would notice.
#[test]
fn multi_lane_watermarks_pair_each_lane_with_its_own_key() {
    let signer = Address::repeat_byte(0x5E);
    let lanes = [lane_wm(1, 0xA1, 0, 100, 200), lane_wm(1, 0xB2, 0, 300, 400)];
    let out = super::multi_lane_watermarks(signer, &lanes);
    assert_eq!(out.len(), 2);

    assert_eq!(out[0].0.provider, Address::repeat_byte(0xA1));
    assert_eq!(out[0].0.signer, signer);
    assert_eq!(out[0].0.pool_id, PoolId::repeat_byte(1));
    assert_eq!(
        out[0].1.advanced(),
        Some((U256::from(100u64), U256::from(200u64))),
        "lane A carries lane A's cumulative"
    );

    assert_eq!(out[1].0.provider, Address::repeat_byte(0xB2));
    assert_eq!(
        out[1].1.advanced(),
        Some((U256::from(300u64), U256::from(400u64))),
        "lane B carries lane B's cumulative"
    );
}

/// A multi-source lane settles at its ARMED cumulative, never at `committed`.
/// A cancelled or stalled leg drops its `fill_gap` future wherever it is
/// parked — including inside the voucher exchange `issue` deliberately arms
/// before sending — and that can happen on a SUCCESSFUL fetch. Settling that
/// lane low persists a cumulative below what the node can redeem, and the next
/// fetch on the lane signs a value the upstream already holds: rejected as a
/// regression.
#[test]
fn multi_lane_watermarks_settle_high_even_when_the_fetch_succeeded() {
    let signer = Address::repeat_byte(0x5E);
    // The armed cumulative sits ABOVE what was acked — the dropped-leg
    // shape. Settling at `committed` would persist the lower one.
    let lanes = [lane_wm(1, 0xA1, 0, 300, 400)];
    let out = super::multi_lane_watermarks(signer, &lanes);
    assert_eq!(
        out[0].1.advanced(),
        Some((U256::from(300u64), U256::from(400u64))),
        "the armed (settlement) cumulative is what gets persisted"
    );
}

/// A lane that advanced nothing persists nothing: `advanced()` is `None`, and
/// `persist_watermark` returns early rather than writing a no-op row.
#[test]
fn multi_lane_watermarks_report_no_advance_for_an_untouched_lane() {
    let signer = Address::repeat_byte(0x5E);
    let lanes = [lane_wm(1, 0xA1, 200, 0, 200)];
    let out = super::multi_lane_watermarks(signer, &lanes);
    assert_eq!(
        out[0].1.advanced(),
        None,
        "a lane at its prior amount has not advanced"
    );
}

/// A usable rate renders as a human `X/s`; a sub-1-byte/s rate (no data yet,
/// or a stall) and any non-finite value both render as `--`.
#[test]
fn fmt_rate_shows_human_units_and_placeholder_below_one() {
    assert!(fmt_rate(2.0 * 1024.0 * 1024.0).ends_with("/s"));
    assert!(fmt_rate(2.0 * 1024.0 * 1024.0).contains("MiB"));
    assert_eq!(fmt_rate(0.0), "--");
    assert_eq!(fmt_rate(0.4), "--");
    assert_eq!(fmt_rate(f64::NAN), "--");
    assert_eq!(fmt_rate(f64::INFINITY), "--");
}

/// ETA divides remaining bytes by the smoothed rate; below a usable rate it
/// reports `ETA --` rather than a divide-by-tiny blow-up, and a huge
/// projection is clamped so `Duration::from_secs_f64` cannot overflow.
#[test]
fn fmt_eta_projects_and_guards_low_rate() {
    assert_eq!(fmt_eta(10 << 20, 0.0), "ETA --");
    assert_eq!(fmt_eta(10 << 20, 0.9), "ETA --");
    assert!(fmt_eta(10 << 20, 10.0 * 1024.0 * 1024.0).starts_with("ETA "));
    // A near-zero rate with bytes left must not panic on the clamp path.
    let _ = fmt_eta(u64::MAX, 1.0);
}

/// The summary measures elapsed and bytes-moved from the FIRST observed
/// sample, not the final position — so a resumed fetch that began at a
/// non-zero `base_present` reports only what this run actually transferred.
#[test]
fn summary_reports_delta_from_first_sample_not_absolute_position() {
    let t0 = Instant::now();
    let state = SpeedState {
        // Resumed at 40 MiB already present, ran for 2s to 60 MiB.
        started: Some((t0, 40 << 20)),
        last: Some((t0 + Duration::from_secs(2), 60 << 20)),
        ewma_bps: None,
    };
    let meter = DeliveryMeter {
        state: Arc::new(Mutex::new(state)),
    };
    let (elapsed, moved) = meter
        .summary()
        .expect("a delivered sample yields a summary");
    assert_eq!(elapsed, Duration::from_secs(2));
    assert_eq!(
        moved,
        20 << 20,
        "only this run's 20 MiB, not the 60 MiB total"
    );
}

/// No delivery ever observed (a failure before the first byte) yields no
/// summary, so the caller falls back to the bare byte/output line.
#[test]
fn summary_is_none_before_any_delivery() {
    let meter = DeliveryMeter {
        state: Arc::new(Mutex::new(SpeedState::default())),
    };
    assert!(meter.summary().is_none());
}
