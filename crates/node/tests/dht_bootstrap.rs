//! Bootstrap / republish loopback (PR 4 of #320, ADR 022 §Bootstrap
//! and §STORE Flow).
//!
//! Spins up a server endpoint running the full DHT handler stack
//! (routing table, record store, staker filter), then drives the
//! *client* side via [`decdn_node::dht::client`] and the bootstrap
//! orchestrator. The tests pin three contracts:
//!
//! - `client::find_node` against a live server returns a
//!   `FindNodeResponse` and refreshes the requester into the server's
//!   routing table (the server-side `note_peer_seen` from PR 2).
//! - `bootstrap::bootstrap` seeds the requester's routing table from
//!   the staker set AND populates it with the seed's closer-nodes.
//! - `client::store` against a staked publisher's server lands a
//!   record in the server's `RecordStore`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};

use decdn_common::config::ResolvedSecurity;
use decdn_node::dht::{
    ConfigStakerSet, DhtRateLimiter, RecordStore, RecordStoreConfig, RoutingTable, StakerSet,
    bootstrap, client, rate_limit::DhtRateLimitConfig,
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
        trusted_ips: std::collections::HashSet::new(),
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

/// Server handle: holds the endpoint, the accept task, and shared
/// handles to inspect the server-side routing table and record store
/// from the test.
struct TestServer {
    endpoint: Endpoint,
    addr: SocketAddr,
    id: iroh::PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    records: Arc<Mutex<RecordStore>>,
    accept_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Spin up a DHT server with the given staker set + an empty routing
/// table. Returns a [`TestServer`] handle.
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
        // Accept loop — single connection per test is sufficient.
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

#[tokio::test(flavor = "multi_thread")]
async fn client_find_node_roundtrip_via_dht_client_module() -> anyhow::Result<()> {
    // Server with an empty routing table; client uses the new
    // `dht::client::find_node` primitive.
    let server = spin_up_server(HashSet::new()).await?;
    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server.id).with_ip_addr(server.addr);
    let target_id = [0xABu8; 32];
    let resp = client::find_node(&client_ep, target, target_id, *client_ep.id().as_bytes()).await?;
    assert_eq!(resp.target, target_id, "target echoed");
    // The server's `serve()` refreshed the authenticated client
    // NodeId into its routing table BEFORE dispatching this request,
    // so the client itself appears in the response's `closer_nodes`
    // (modulo the server's own id, which `closest()` filters).
    assert_eq!(
        resp.closer_nodes,
        vec![*client_ep.id().as_bytes()],
        "the freshly-refreshed client must appear as the only closer node"
    );
    // Server should have refreshed the authenticated client NodeId
    // into its routing table (PR 2 behaviour, unchanged by PR 4).
    let in_table = {
        let t = server.routing.lock().unwrap();
        t.contains(client_ep.id().as_bytes())
    };
    assert!(in_table, "server must refresh client NodeId on FindNode");

    client_ep.close().await;
    server.endpoint.close().await;
    server.accept_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_seeds_routing_and_runs_self_lookup() -> anyhow::Result<()> {
    // Server S accepts DHT connections. Client C bootstraps with S as
    // the only seed.
    //
    // After bootstrap:
    // - C's routing table contains S (step 1 — seed insert).
    // - S's routing table contains C (step 2 — C's FindNode against S
    //   refreshes C's authenticated NodeId server-side).
    let server = spin_up_server(HashSet::new()).await?;
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    // Seed: only the server's NodeId.
    let mut staked = HashSet::new();
    staked.insert(*server.id.as_bytes());
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
    let client_routing = Arc::new(Mutex::new(RoutingTable::new(*client_id.as_bytes())));

    // iroh's `Endpoint::connect` without an explicit addr requires
    // discovery; loopback tests disable relay/discovery, so we
    // pre-seed iroh's known-addresses for the server NodeId by
    // exercising one FindNode through the explicit-addr target. After
    // that the client's iroh cache has the server's path and
    // bootstrap's `EndpointAddr::new(node_id)` (no IP) can resolve it.
    {
        let target = EndpointAddr::new(server.id).with_ip_addr(server.addr);
        let _ = client::find_node(
            &client_ep,
            target,
            *server.id.as_bytes(),
            *client_id.as_bytes(),
        )
        .await?;
    }

    let outcome = bootstrap::bootstrap(&client_ep, client_id, &client_routing, &staker_set).await;
    assert_eq!(outcome.seeds_seen, 1);
    assert_eq!(outcome.seeds_inserted, 1, "S goes into C's table");
    assert!(outcome.find_node_ok >= 1, "at least one FindNode succeeded");

    // C's routing table now contains S.
    {
        let t = client_routing.lock().unwrap();
        assert!(t.contains(server.id.as_bytes()));
    }
    // S's routing table now contains C — exercised by the bootstrap's
    // FindNode call against S.
    {
        let t = server.routing.lock().unwrap();
        assert!(t.contains(client_id.as_bytes()));
    }

    client_ep.close().await;
    server.endpoint.close().await;
    server.accept_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn client_store_lands_record_at_server() -> anyhow::Result<()> {
    // Server admits the client as a staked publisher; client.store
    // should successfully insert into the server's RecordStore.
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let mut staked = HashSet::new();
    staked.insert(*client_id.as_bytes());
    let server = spin_up_server(staked).await?;

    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server.id).with_ip_addr(server.addr);
    let hash = [0x77u8; 32];
    let ack = client::store(&client_ep, target, hash, *client_id.as_bytes()).await?;
    assert!(ack.accepted, "staked publisher's Store must be accepted");
    assert_eq!(ack.hash, hash);

    // Server's RecordStore now has the record.
    {
        let r = server.records.lock().unwrap();
        assert_eq!(r.publisher_record_count(client_id.as_bytes()), 1);
        assert_eq!(r.len(), 1);
    }

    client_ep.close().await;
    server.endpoint.close().await;
    server.accept_task.abort();
    Ok(())
}
