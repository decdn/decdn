//! End-to-end integration coverage for the iterative `FindValue`
//! lookup path introduced in PR #673 (ADR 022 §`FIND_VALUE` Flow,
//! §Routing Table, §Lookup integrity). Issue #711.
//!
//! `dht_lookup.rs` already pins the single-hop lookup invariants
//! (happy path, the active-staker and negative-cache response filters,
//! and empty-table convergence; the XOR-closer filter is unit-tested
//! in `dht::lookup::tests`). This file covers the behaviours that only
//! manifest once the routing table and negative cache interact with a
//! live, possibly multi-round, lookup:
//!
//! 1. **Bucket-overflow eviction + node departure + convergence.**
//!    The routing table is *not* classic split-on-overflow Kademlia —
//!    `routing.rs` keeps one fixed bucket per XOR-prefix length and
//!    evicts the least-recently-seen entry on overflow (see its module
//!    docs). This test fills a single bucket past `K_BUCKET_SIZE`,
//!    confirms the LRU entries are evicted, removes a surviving entry
//!    (a departing node), and then drives a real `find_providers` to
//!    confirm the lookup still converges through the remaining peer.
//!
//! 2. **Unreachable peer mid-lookup.** A peer that was reachable when
//!    seeded into the routing table but whose handler is dropped
//!    before the lookup runs must not wedge the pass — even when that
//!    dead peer is the *closest* candidate to the target: the lookup
//!    absorbs the transport failure, converges to the reachable
//!    (farther) provider, and never surfaces the dead peer. (The "a
//!    queried peer is not re-queried within the same pass" invariant is
//!    pinned by the `dht::lookup::tests` unit test
//!    `lookup_state_pick_returns_closest_alpha_marks_queried`; there is
//!    no routing-table mutation on failure — the table is left
//!    untouched, matching the implementation.)
//!
//! 3. **Multi-round convergence via `closer_nodes`.** A lookup whose
//!    seed peer holds no record but routes a strictly-closer
//!    `closer_node` toward the actual holder must run a *second* round
//!    against that holder and converge. This exercises the iterative
//!    `observed_closer` loop in `find_providers` end-to-end — the core
//!    of the feature, otherwise only covered by the pure `fold_response`
//!    unit tests.
//!
//! 4. **Negative-cache TTL expiry.** A `(provider, target)` pair in
//!    the negative cache suppresses the provider on the first lookup,
//!    but once its TTL elapses the entry is swept and the *same* cache
//!    no longer suppresses the provider — expiry must not poison
//!    future lookups. The negative cache is anchored on
//!    `std::time::Instant`, so `tokio::time` pause/advance cannot drive
//!    it; the test injects a short TTL via
//!    `NegativeProbeCache::with_capacity_and_ttl` and waits on the real
//!    clock, in the same generous-wall-clock style as the
//!    `negative_cache` unit tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use decdn_common::config::ResolvedSecurity;
use decdn_node::dht::routing::K_BUCKET_SIZE;
use decdn_node::dht::{
    ConfigStakerSet, DhtRateLimiter, InsertOutcome, LookupConfig, NegativeProbeCache, RecordStore,
    RecordStoreConfig, RoutingTable, StakerSet, client, find_providers,
    rate_limit::DhtRateLimitConfig,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::dht::DhtHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{ALPN_DHT, ContentHash, NodeId};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, endpoint::presets};

fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

fn permissive_limiter(metrics: &Arc<Metrics>) -> Arc<ConnectionLimiter> {
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1_000_000.0,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(metrics)))
}

fn permissive_dht_rate_limiter(metrics: &Arc<Metrics>) -> Arc<DhtRateLimiter> {
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1e6,
        per_peer_burst: u32::MAX,
        per_ip_rate_per_sec: 1e6,
        per_ip_burst: u32::MAX,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        max_tracked_per_ip: 4096,
        max_tracked_per_peer: 4096,
    };
    Arc::new(DhtRateLimiter::new(&cfg, Arc::clone(metrics)))
}

async fn local_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, SocketAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(bind)
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind: {e}"))?;
    let addr = ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let addr = match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };
    Ok((ep, addr))
}

/// Server handle: holds the endpoint, accept task, and shared routing
/// / record handles so tests can seed them before issuing lookups.
#[allow(dead_code)]
struct TestServer {
    endpoint: Endpoint,
    addr: SocketAddr,
    id: iroh::PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    records: Arc<Mutex<RecordStore>>,
    accept_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    /// Abort the accept loop and close the endpoint, awaiting the
    /// close so the peer is genuinely unreachable by the time this
    /// returns — the "handler dropped mid-lookup" simulation in
    /// `find_providers_tolerates_unreachable_peer` relies on this.
    async fn shutdown(self) {
        self.accept_task.abort();
        self.endpoint.close().await;
    }

    /// Fire-and-forget close for servers whose teardown ordering
    /// doesn't matter to the assertion.
    fn shutdown_detached(self) {
        self.accept_task.abort();
        let ep = self.endpoint.clone();
        tokio::spawn(async move { ep.close().await });
    }
}

/// Spin up a DHT server with the given staker set + an empty routing
/// table + empty record store. Tests can mutate `routing` and
/// `records` via the returned handle.
async fn spin_up_server(staked: HashSet<[u8; 32]>) -> anyhow::Result<TestServer> {
    let secret = fresh_key();
    let id = secret.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let rate_limiter = permissive_dht_rate_limiter(&metrics);
    let staked: HashSet<NodeId> = staked.into_iter().map(NodeId::from_bytes).collect();
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *id.as_bytes(),
    ))));
    let records = Arc::new(Mutex::new(RecordStore::new(RecordStoreConfig::default())));

    let handler = Arc::new(DhtHandler::with_routing(
        id,
        Arc::clone(&routing),
        rate_limiter,
        limiter,
        Arc::clone(&metrics),
        staker_set,
        Arc::clone(&records),
    ));
    let (endpoint, addr) = local_endpoint(secret, vec![ALPN_DHT.to_vec()]).await?;
    let endpoint_bg = endpoint.clone();
    let accept_task = tokio::spawn(async move {
        while let Some(incoming) = endpoint_bg.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else {
                continue;
            };
            let h = Arc::clone(&handler);
            tokio::spawn(async move {
                let _ = h.accept(conn).await;
            });
        }
        Ok::<_, anyhow::Error>(())
    });
    Ok(TestServer {
        endpoint,
        addr,
        id,
        routing,
        records,
        accept_task,
    })
}

/// Prime the client's iroh address cache for `server` by issuing one
/// explicit-addr `find_node` against it. After this, the client can
/// dial `EndpointAddr::new(server.id)` without an explicit address
/// (which is what the lookup module does for routing-table candidates
/// and closer-nodes returned over the wire).
async fn prime_iroh_cache(
    client_ep: &Endpoint,
    client_id: &[u8; 32],
    server: &TestServer,
) -> anyhow::Result<()> {
    let target = EndpointAddr::new(server.id).with_ip_addr(server.addr);
    let _ = client::find_node(
        client_ep,
        target,
        NodeId::from_bytes(*server.id.as_bytes()),
        NodeId::from_bytes(*client_id),
    )
    .await?;
    Ok(())
}

fn lookup_cfg_for_test() -> LookupConfig {
    // Short round timeout so a wedged test (or the deliberately
    // unreachable peer in `find_providers_tolerates_unreachable_peer`)
    // fails fast instead of waiting out the 8s production default.
    LookupConfig {
        round_timeout: Duration::from_secs(3),
        ..LookupConfig::default()
    }
}

fn insert_record(records: &Mutex<RecordStore>, hash: [u8; 32], holder: [u8; 32]) {
    let mut guard = records.lock().expect("record store mutex poisoned");
    // `receive_us` anchored to now so the record's `now + TTL` expiry
    // is comfortably in the future when the handler queries.
    let receive_us = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX);
    let outcome = guard.insert_at(
        NodeId::from_bytes(holder),
        ContentHash::from_bytes(hash),
        receive_us,
    );
    assert!(
        matches!(outcome, InsertOutcome::Inserted | InsertOutcome::Refreshed),
        "test record insert must succeed: {outcome:?}"
    );
}

/// Build `count` distinct `NodeId`s that all hash into the *same*
/// k-bucket of a table anchored at the all-zero `self_id`.
///
/// The bucket index is the position of the most-significant set bit of
/// the XOR distance (`routing::bucket_index`). With `self_id == [0; 32]`
/// the distance is the id itself, so fixing the first (high-order) byte
/// to a single non-zero value pins the MSB position — every id then
/// lands in one bucket regardless of its lower bytes. We vary the last
/// byte to keep the ids distinct.
fn ids_in_one_bucket(count: usize) -> Vec<[u8; 32]> {
    (0..count)
        .map(|i| {
            let mut id = [0u8; 32];
            id[0] = 0x40; // first non-zero byte ⇒ fixed bucket index.
            id[31] = u8::try_from(i + 1).expect("bucket fill count fits in a u8");
            id
        })
        .collect()
}

/// Scenario 1 (routing mechanics): overflowing one bucket evicts the
/// least-recently-seen entries down to `K_BUCKET_SIZE`, and a
/// surviving entry can then be removed when its node departs.
///
/// `routing.rs` is LRU-on-overflow, not split-on-overflow Kademlia, so
/// "k-bucket split" from issue #711 maps onto this eviction behaviour.
#[test]
fn bucket_overflow_evicts_lru_and_departed_node_is_removed() {
    let self_id = [0u8; 32];
    let mut table = RoutingTable::new(NodeId::from_bytes(self_id));

    let overflow = K_BUCKET_SIZE + 2;
    let ids: Vec<NodeId> = ids_in_one_bucket(overflow)
        .into_iter()
        .map(NodeId::from_bytes)
        .collect();
    for id in &ids {
        table.insert(*id);
    }

    // Overflow is capped at K, and the two least-recently-inserted
    // entries were evicted.
    assert_eq!(
        table.len(),
        K_BUCKET_SIZE,
        "bucket must cap at K_BUCKET_SIZE on overflow"
    );
    assert!(!table.contains(&ids[0]), "oldest entry must be evicted");
    assert!(
        !table.contains(&ids[1]),
        "second-oldest entry must be evicted"
    );
    assert!(
        table.contains(&ids[overflow - 1]),
        "most-recently-inserted entry must survive"
    );

    // A surviving node departs the network: remove it from the table.
    let departed = ids[overflow - 1];
    assert!(table.remove(&departed), "remove must report a hit");
    assert!(
        !table.contains(&departed),
        "departed node must be gone from the table"
    );
    assert_eq!(table.len(), K_BUCKET_SIZE - 1);
    // Removing an already-absent node is a no-op miss.
    assert!(!table.remove(&departed));
}

/// Scenario 1 (convergence): after a peer departs (is removed from the
/// routing table), a `find_providers` lookup still converges to the
/// reachable provider through the surviving entry.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_converges_after_peer_departs() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xA1; 32];

    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let provider = spin_up_server(HashSet::new()).await?;
    insert_record(&provider.records, target, *provider.id.as_bytes());
    prime_iroh_cache(&client_ep, client_id.as_bytes(), &provider).await?;

    // A second peer departs the network before the lookup. It is
    // staked and was in the routing table, but is then removed.
    let departed_id = NodeId::from_bytes(*fresh_key().public().as_bytes());

    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    {
        let mut t = routing.lock().unwrap();
        t.insert(NodeId::from_bytes(*provider.id.as_bytes()));
        t.insert(departed_id);
        assert!(t.contains(&departed_id));
        assert!(t.remove(&departed_id), "departing peer removed");
        assert!(!t.contains(&departed_id));
        assert!(
            t.contains(&NodeId::from_bytes(*provider.id.as_bytes())),
            "provider survives"
        );
    }

    let mut client_staked = HashSet::new();
    client_staked.insert(NodeId::from_bytes(*provider.id.as_bytes()));
    client_staked.insert(departed_id);
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(client_staked));
    let neg = NegativeProbeCache::new();

    let providers = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        NodeId::from_bytes(*client_id.as_bytes()),
        ContentHash::from_bytes(target),
        lookup_cfg_for_test(),
        None,
    )
    .await;

    assert_eq!(
        providers,
        vec![NodeId::from_bytes(*provider.id.as_bytes())],
        "lookup must converge to the surviving provider after a departure"
    );

    client_ep.close().await;
    provider.shutdown_detached();
    Ok(())
}

/// Scenario 2: a peer that becomes unreachable mid-lookup (its handler
/// dropped and endpoint closed) does not wedge the pass — even when it
/// is the *closest* candidate to the target. The lookup absorbs the
/// transport failure, converges to the reachable (farther) provider,
/// and never surfaces the dead peer.
///
/// Making the dead peer the closest candidate is what gives the test
/// teeth: an implementation that gave up the moment its nearest peer
/// failed to answer would return empty here, where a correct one still
/// folds in the live provider's record. The `find_providers` call is
/// bounded by a wall-clock guard so a regression that *hangs* on an
/// unresponsive peer surfaces as a clear failure rather than a stuck
/// test (the lookup legitimately waits out one `round_timeout` while
/// the dead peer's dial fails, then converges).
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_tolerates_unreachable_peer() -> anyhow::Result<()> {
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    // A peer that is reachable just long enough to be primed into the
    // client's address cache, then has its handler dropped — it becomes
    // unreachable before the lookup runs. The lookup target is the dead
    // peer's own id, so the dead peer is the closest possible candidate
    // (XOR distance 0) and is dialed first.
    let dead = spin_up_server(HashSet::new()).await?;
    let target: [u8; 32] = *dead.id.as_bytes();
    let dead_id = NodeId::from_bytes(target);
    prime_iroh_cache(&client_ep, client_id.as_bytes(), &dead).await?;

    // Reachable provider holding the record for `target`. It is farther
    // from `target` than the dead peer, so a lookup that bailed on the
    // closest peer's failure would never reach it.
    let provider = spin_up_server(HashSet::new()).await?;
    insert_record(&provider.records, target, *provider.id.as_bytes());
    prime_iroh_cache(&client_ep, client_id.as_bytes(), &provider).await?;

    dead.shutdown().await;

    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    {
        let mut t = routing.lock().unwrap();
        t.insert(NodeId::from_bytes(*provider.id.as_bytes()));
        t.insert(dead_id);
    }

    let mut client_staked = HashSet::new();
    client_staked.insert(NodeId::from_bytes(*provider.id.as_bytes()));
    client_staked.insert(dead_id);
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(client_staked));
    let neg = NegativeProbeCache::new();

    // Guard against a hang regression: the lookup waits out at most one
    // `round_timeout` (3s) on the dead peer, so 12s is comfortable
    // headroom while still failing fast on an unbounded stall.
    let providers = tokio::time::timeout(
        Duration::from_secs(12),
        find_providers(
            &client_ep,
            &routing,
            &staker_set,
            &neg,
            NodeId::from_bytes(*client_id.as_bytes()),
            ContentHash::from_bytes(target),
            lookup_cfg_for_test(),
            None,
        ),
    )
    .await
    .expect("lookup must not hang when a queried peer is unreachable");

    assert_eq!(
        providers,
        vec![NodeId::from_bytes(*provider.id.as_bytes())],
        "lookup must converge to the reachable provider despite the closest peer being dead"
    );
    assert!(
        !providers.contains(&dead_id),
        "the unreachable peer must never appear as a provider"
    );

    client_ep.close().await;
    provider.shutdown_detached();
    Ok(())
}

/// Scenario 3: a genuine two-round lookup. The client's only routing
/// entry is a `seed` peer that holds no record but knows a
/// strictly-closer `holder`. Round 1 against `seed` yields no provider
/// but a `closer_node`; the iterative loop must then run a *second*
/// round against `holder` and converge to it. Exercises the
/// `observed_closer` loop in `find_providers` end-to-end.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_converges_via_closer_nodes_second_round() -> anyhow::Result<()> {
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    // The holder owns the record. Targeting the holder's own id makes it
    // the closest possible node to `target`, so `seed`'s referral
    // survives the XOR-closer filter (Filter 1).
    let holder = spin_up_server(HashSet::new()).await?;
    let holder_bytes = *holder.id.as_bytes();
    let holder_id = NodeId::from_bytes(holder_bytes);
    let target: [u8; 32] = holder_bytes;
    insert_record(&holder.records, target, holder_bytes);
    prime_iroh_cache(&client_ep, client_id.as_bytes(), &holder).await?;

    // The seed holds no record but knows the holder — its FindValue
    // response carries `holder` in `closer_nodes`, driving round 2.
    let seed = spin_up_server(HashSet::new()).await?;
    let seed_id = NodeId::from_bytes(*seed.id.as_bytes());
    seed.routing.lock().unwrap().insert(holder_id);
    prime_iroh_cache(&client_ep, client_id.as_bytes(), &seed).await?;

    // The client's routing table contains only the seed — the holder is
    // reachable solely via the seed's referral.
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    routing.lock().unwrap().insert(seed_id);

    let mut client_staked = HashSet::new();
    client_staked.insert(seed_id);
    client_staked.insert(holder_id);
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(client_staked));
    let neg = NegativeProbeCache::new();

    let providers = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        NodeId::from_bytes(*client_id.as_bytes()),
        ContentHash::from_bytes(target),
        lookup_cfg_for_test(),
        None,
    )
    .await;

    assert_eq!(
        providers,
        vec![holder_id],
        "lookup must follow the seed's closer_node to the holder and converge"
    );

    client_ep.close().await;
    seed.shutdown_detached();
    holder.shutdown_detached();
    Ok(())
}

/// Scenario 4: a negative-cache entry suppresses a provider only until
/// its TTL elapses. After expiry, the *same* cache no longer suppresses
/// the provider — a stale entry must not poison future lookups.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_negative_cache_expires_after_ttl() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xC3; 32];

    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let provider = spin_up_server(HashSet::new()).await?;
    insert_record(&provider.records, target, *provider.id.as_bytes());
    prime_iroh_cache(&client_ep, client_id.as_bytes(), &provider).await?;

    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    routing
        .lock()
        .unwrap()
        .insert(NodeId::from_bytes(*provider.id.as_bytes()));

    let mut client_staked = HashSet::new();
    client_staked.insert(NodeId::from_bytes(*provider.id.as_bytes()));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(client_staked));

    // Short TTL injected via the test/tuning seam. The cache is anchored
    // on `Instant`, so this waits on the real clock (TTL 750ms, sleep
    // 1100ms) in the same generous-wall-clock style as the
    // `negative_cache` unit tests. The 1100ms sleep alone exceeds the
    // 750ms TTL, so the margin doesn't depend on the first lookup's
    // duration.
    let ttl = Duration::from_millis(750);
    let neg = NegativeProbeCache::with_capacity_and_ttl(16, ttl);
    neg.record_failure(
        NodeId::from_bytes(*provider.id.as_bytes()),
        ContentHash::from_bytes(target),
    );

    // First lookup: the pre-recorded failure suppresses the provider.
    let suppressed = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        NodeId::from_bytes(*client_id.as_bytes()),
        ContentHash::from_bytes(target),
        lookup_cfg_for_test(),
        None,
    )
    .await;
    assert!(
        suppressed.is_empty(),
        "provider must be suppressed while the negative entry is live: {suppressed:?}"
    );

    // Let the negative entry expire.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Second lookup with the SAME cache: the entry has expired, so the
    // provider is no longer suppressed.
    let recovered = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        NodeId::from_bytes(*client_id.as_bytes()),
        ContentHash::from_bytes(target),
        lookup_cfg_for_test(),
        None,
    )
    .await;
    assert_eq!(
        recovered,
        vec![NodeId::from_bytes(*provider.id.as_bytes())],
        "expired negative entry must not poison the follow-up lookup"
    );

    client_ep.close().await;
    provider.shutdown_detached();
    Ok(())
}
