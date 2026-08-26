//! Iterative `FindValue` lookup loopback (ADR 022
//! §`FIND_VALUE` Flow + §Lookup integrity).
//!
//! Spins up one or more DHT-server endpoints with seeded routing
//! tables / record stores, then drives [`decdn_node::dht::find_providers`]
//! from a client endpoint. The tests pin the four lookup invariants:
//!
//! 1. Happy path — a provider reachable through the local routing
//!    table appears in the result.
//! 2. Filter 2 (active-staker) — a non-staked `NodeId` in the response
//!    is silently dropped.
//! 3. Filter 3 (negative cache) — a pre-recorded `(provider, target)`
//!    pair is silently dropped.
//! 4. Convergence — an empty routing table returns an empty result
//!    without panicking.
//!
//! Filter 1 (XOR-closer) is exercised by a dedicated unit test in
//! `dht::lookup::tests`; testing it end-to-end would require a custom
//! handler that emits dishonest `closer_nodes`, which is more
//! complexity than payoff at the loopback layer.

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
use decdn_node::dht::{
    ConfigStakerSet, DhtRateLimiter, InsertOutcome, LookupConfig, NegativeProbeCache, RecordStore,
    RecordStoreConfig, RoutingTable, StakerSet, client, find_providers, lookup::MAX_LOOKUP_ROUNDS,
    rate_limit::DhtRateLimitConfig,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::dht::DhtHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{ALPN_DHT, ContentHash, NodeId};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, endpoint::presets};

// Teardown is routed through the shared bounded helper rather than a bare
// `Endpoint::close().await`, which has no deadline of its own. Called fully
// qualified: `TestServer` carries a `shutdown` method of its own, and a bare
// `shutdown(...)` beside `provider.shutdown()` reads ambiguously.
mod support;

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

/// Server handle: holds the endpoint, accept task, and shared
/// routing / record handles so tests can seed them before issuing
/// lookups.
#[allow(dead_code)]
struct TestServer {
    endpoint: Endpoint,
    addr: SocketAddr,
    id: iroh::PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    records: Arc<Mutex<RecordStore>>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// The two halves teardown needs: the accept loop's handle and the
    /// endpoint it accepts on. A test that tears several servers down
    /// together hands these to one [`support::shutdown`] call, so the
    /// whole teardown costs one deadline rather than one per endpoint.
    ///
    /// The routing / record handles belong to the test, not to teardown,
    /// and are dropped here.
    fn into_teardown_parts(self) -> (tokio::task::JoinHandle<()>, Endpoint) {
        (self.accept_task, self.endpoint)
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
/// (which is what the lookup module does for closer-nodes returned
/// over the wire).
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
    // Short round timeout so a wedged test fails fast instead of
    // hanging out for the 8s production default.
    LookupConfig {
        round_timeout: Duration::from_secs(3),
        ..LookupConfig::default()
    }
}

fn insert_record(records: &Mutex<RecordStore>, hash: [u8; 32], holder: [u8; 32]) {
    let mut guard = records.lock().expect("record store mutex poisoned");
    // `receive_us` anchored to now so the record's `now + TTL`
    // expiry is comfortably in the future when the handler queries.
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

/// Happy path: a provider reachable in one hop from the client's
/// local routing table is returned.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_returns_directly_reachable_provider() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xAA; 32];

    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    // Server staker set doesn't affect FindValue admission — it
    // gates STORE only. Spin up a fresh server and inject the
    // record directly.
    let provider = spin_up_server(HashSet::new()).await?;
    insert_record(&provider.records, target, *provider.id.as_bytes());

    prime_iroh_cache(&client_ep, client_id.as_bytes(), &provider).await?;

    // Client's local routing table has the provider as its only peer.
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    routing
        .lock()
        .unwrap()
        .insert(NodeId::from_bytes(*provider.id.as_bytes()));

    // Client's view of the staker set: trust the provider.
    let mut client_staked = HashSet::new();
    client_staked.insert(NodeId::from_bytes(*provider.id.as_bytes()));
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

    assert_eq!(providers, vec![NodeId::from_bytes(*provider.id.as_bytes())]);

    let (provider_task, provider_ep) = provider.into_teardown_parts();
    support::shutdown([provider_task], [&client_ep, &provider_ep]).await?;
    Ok(())
}

/// Filter 3: a `(provider, target)` pair pre-loaded into the
/// negative cache is dropped, so the same setup as the happy path
/// returns empty.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_drops_providers_in_negative_cache() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xBB; 32];

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
    let neg = NegativeProbeCache::new();
    // Pre-record a failure for (provider, target).
    neg.record_failure(
        NodeId::from_bytes(*provider.id.as_bytes()),
        ContentHash::from_bytes(target),
    );

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

    assert!(
        providers.is_empty(),
        "provider in negative cache must be dropped: got {providers:?}"
    );

    let (provider_task, provider_ep) = provider.into_teardown_parts();
    support::shutdown([provider_task], [&client_ep, &provider_ep]).await?;
    Ok(())
}

/// Filter 2: a provider whose `NodeId` is NOT in the staker set is
/// dropped from the wire response, even though the responder server
/// has a record advertising it.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_drops_non_staked_provider() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xCC; 32];

    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    // Provider responds with a record, but the client's view of the
    // staker set DOES NOT include it. The server's own staker set
    // (used for STORE admission, not relevant here) is fine.
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

    // Client's staker set is empty — every peer is filtered out.
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::empty());
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

    assert!(
        providers.is_empty(),
        "non-staked provider must be dropped: got {providers:?}"
    );

    let (provider_task, provider_ep) = provider.into_teardown_parts();
    support::shutdown([provider_task], [&client_ep, &provider_ep]).await?;
    Ok(())
}

/// Convergence with an empty routing table — no panic, empty return.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_with_empty_routing_table_returns_empty() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xDD; 32];
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::empty());
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

    assert!(providers.is_empty());

    support::shutdown([], [&client_ep]).await?;
    Ok(())
}

/// XOR distance between a node id and the target, as a big-endian magnitude — the same
/// ordering the routing table uses. Only comparisons matter here.
fn xor_distance(id: &[u8; 32], target: &[u8; 32]) -> [u8; 32] {
    let mut d = [0u8; 32];
    for i in 0..32 {
        // Indexing is bounded by the array length on both sides.
        if let (Some(slot), Some(a), Some(b)) = (d.get_mut(i), id.get(i), target.get(i)) {
            *slot = a ^ b;
        }
    }
    d
}

/// `find_providers` must TERMINATE at `MAX_LOOKUP_ROUNDS`, even while every round is still
/// finding strictly-closer nodes (#1145 review).
///
/// This is the load-bearing half of the outer-pull-deadline formula, and it had no test at
/// all. `PULL_THROUGH_OUTER_SLACK` is now DERIVED as
/// `PROBE_TIMEOUT + DEFAULT_ROUND_TIMEOUT × MAX_LOOKUP_ROUNDS`, so the slack is only a valid
/// budget if the lookup really honours that ceiling. An arithmetic assertion cannot check
/// that — it would just restate the definition (`A >= A`), which is exactly what the test
/// this replaces did. The only thing that can break is the LOOP, so the loop is what is
/// driven.
///
/// The fixture is a CHAIN: six servers ordered by XOR distance to the target, each one's
/// routing table holding only the next-closer node. A chain reveals exactly one new
/// candidate per round, so walking it end to end needs six rounds — two more than the
/// ceiling allows.
///
/// The record sits on the LAST server, deliberately out of reach. That makes the assertion
/// behavioural rather than a counter check: the lookup must come back EMPTY, because it was
/// cut off before it got there. Remove the ceiling and the walk continues, the record is
/// found, and this test fails on a non-empty result — which is the point. It fails on the
/// revert in both directions: the ceiling counter also stops firing.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_stops_at_the_round_ceiling_even_while_still_finding_closer_nodes()
-> anyhow::Result<()> {
    let target: [u8; 32] = [0x5C; 32];

    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _client_addr) = local_endpoint(client_sk, vec![ALPN_DHT.to_vec()]).await?;

    // Six servers, then SORTED by distance to the target — so the chain is strictly
    // converging without having to grind keys for it. Six because the ceiling is four: the
    // walk must want more rounds than it is allowed.
    let mut servers = Vec::new();
    for _ in 0..6 {
        servers.push(spin_up_server(HashSet::new()).await?);
    }
    servers.sort_by_key(|s| xor_distance(s.id.as_bytes(), &target));
    // Farthest first: the client starts at the far end and walks inward.
    servers.reverse();

    // Every server is staked and dialable-by-id from the client's view.
    let mut staked = HashSet::new();
    for s in &servers {
        staked.insert(NodeId::from_bytes(*s.id.as_bytes()));
        prime_iroh_cache(&client_ep, client_id.as_bytes(), s).await?;
    }
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));

    // The chain: each server knows only the next, strictly-closer one.
    for i in 0..servers.len() - 1 {
        let (Some(here), Some(next)) = (servers.get(i), servers.get(i + 1)) else {
            anyhow::bail!("chain index out of range");
        };
        here.routing
            .lock()
            .expect("routing mutex poisoned")
            .insert(NodeId::from_bytes(*next.id.as_bytes()));
    }

    // The prize, placed beyond the ceiling's reach: only a lookup that walks the WHOLE chain
    // ever sees it.
    let last = servers.last().expect("six servers");
    insert_record(&last.records, target, *last.id.as_bytes());

    // The client knows only the far end of the chain.
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));
    let first = servers.first().expect("six servers");
    routing
        .lock()
        .expect("routing mutex poisoned")
        .insert(NodeId::from_bytes(*first.id.as_bytes()));

    let neg = NegativeProbeCache::new();
    let metrics = Arc::new(Metrics::new());
    let providers = tokio::time::timeout(
        Duration::from_mins(1),
        find_providers(
            &client_ep,
            &routing,
            &staker_set,
            &neg,
            NodeId::from_bytes(*client_id.as_bytes()),
            ContentHash::from_bytes(target),
            lookup_cfg_for_test(),
            Some(&metrics),
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "the lookup never returned: with no round ceiling, a chain that keeps revealing \
             closer nodes has no bound at all — and it runs inside the caller's outer pull \
             deadline"
        )
    })?;

    // Cut off before the record: the ceiling held.
    assert!(
        providers.is_empty(),
        "the lookup reached a record six hops away, so it ran more than MAX_LOOKUP_ROUNDS \
         ({MAX_LOOKUP_ROUNDS}) rounds — the bound `PULL_THROUGH_OUTER_SLACK` is derived from \
         does not hold, and the outer pull deadline is budgeting for a cost discovery can \
         exceed. Got {providers:?}"
    );

    // …and said so. The counter is the operator's only signal that their round ceiling is
    // too low for their network size; a `debug!` is invisible at the default RUST_LOG=info.
    let encoded = metrics.encode().expect("metrics encode");
    assert!(
        encoded.contains("decdn_dht_lookup_round_ceiling_total 1"),
        "a truncated lookup must be metered, not just logged at debug!; got:\n{encoded}"
    );

    let mut server_tasks = Vec::new();
    let mut server_eps = Vec::new();
    for s in servers {
        let (task, ep) = s.into_teardown_parts();
        server_tasks.push(task);
        server_eps.push(ep);
    }
    let tasks: [tokio::task::JoinHandle<()>; 6] = server_tasks
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected six server tasks"))?;
    let mut eps = vec![&client_ep];
    eps.extend(server_eps.iter());
    let eps: [&Endpoint; 7] = eps
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected seven endpoints"))?;
    support::shutdown(tasks, eps).await?;
    Ok(())
}
