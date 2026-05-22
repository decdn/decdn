//! Iterative `FindValue` lookup loopback (PR 5 of #320, ADR 022
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
    RecordStoreConfig, RoutingTable, StakerSet, client, find_providers,
    rate_limit::DhtRateLimitConfig,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::dht::DhtHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::ALPN_DHT;
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
        trusted_ips: HashSet::new(),
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
#[allow(dead_code)] // `routing` is held so tests CAN inspect server-side state if needed; not all tests do.
struct TestServer {
    endpoint: Endpoint,
    addr: SocketAddr,
    id: iroh::PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    records: Arc<Mutex<RecordStore>>,
    accept_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    fn shutdown(self) {
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
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
    let routing = Arc::new(Mutex::new(RoutingTable::new(*id.as_bytes())));
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
/// (which is what the lookup module does for closer-nodes returned
/// over the wire).
async fn prime_iroh_cache(
    client_ep: &Endpoint,
    client_id: &[u8; 32],
    server: &TestServer,
) -> anyhow::Result<()> {
    let target = EndpointAddr::new(server.id).with_ip_addr(server.addr);
    let _ = client::find_node(client_ep, target, *server.id.as_bytes(), *client_id).await?;
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
    let outcome = guard.insert_at(holder, hash, receive_us);
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
    let routing = Arc::new(Mutex::new(RoutingTable::new(*client_id.as_bytes())));
    routing.lock().unwrap().insert(*provider.id.as_bytes());

    // Client's view of the staker set: trust the provider.
    let mut client_staked = HashSet::new();
    client_staked.insert(*provider.id.as_bytes());
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(client_staked));
    let neg = NegativeProbeCache::new();

    let providers = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        *client_id.as_bytes(),
        target,
        lookup_cfg_for_test(),
    )
    .await;

    assert_eq!(providers, vec![*provider.id.as_bytes()]);

    client_ep.close().await;
    provider.shutdown();
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
    let routing = Arc::new(Mutex::new(RoutingTable::new(*client_id.as_bytes())));
    routing.lock().unwrap().insert(*provider.id.as_bytes());

    let mut client_staked = HashSet::new();
    client_staked.insert(*provider.id.as_bytes());
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(client_staked));
    let neg = NegativeProbeCache::new();
    // Pre-record a failure for (provider, target).
    neg.record_failure(*provider.id.as_bytes(), target);

    let providers = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        *client_id.as_bytes(),
        target,
        lookup_cfg_for_test(),
    )
    .await;

    assert!(
        providers.is_empty(),
        "provider in negative cache must be dropped: got {providers:?}"
    );

    client_ep.close().await;
    provider.shutdown();
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
    let routing = Arc::new(Mutex::new(RoutingTable::new(*client_id.as_bytes())));
    routing.lock().unwrap().insert(*provider.id.as_bytes());

    // Client's staker set is empty — every peer is filtered out.
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::empty());
    let neg = NegativeProbeCache::new();

    let providers = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        *client_id.as_bytes(),
        target,
        lookup_cfg_for_test(),
    )
    .await;

    assert!(
        providers.is_empty(),
        "non-staked provider must be dropped: got {providers:?}"
    );

    client_ep.close().await;
    provider.shutdown();
    Ok(())
}

/// Convergence with an empty routing table — no panic, empty return.
#[tokio::test(flavor = "multi_thread")]
async fn find_providers_with_empty_routing_table_returns_empty() -> anyhow::Result<()> {
    let target: [u8; 32] = [0xDD; 32];
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let routing = Arc::new(Mutex::new(RoutingTable::new(*client_id.as_bytes())));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::empty());
    let neg = NegativeProbeCache::new();

    let providers = find_providers(
        &client_ep,
        &routing,
        &staker_set,
        &neg,
        *client_id.as_bytes(),
        target,
        lookup_cfg_for_test(),
    )
    .await;

    assert!(providers.is_empty());

    client_ep.close().await;
    Ok(())
}
