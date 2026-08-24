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
    let target_id = NodeId::from_bytes([0xABu8; 32]);
    let resp = client::find_node(
        &client_ep,
        target,
        target_id,
        NodeId::from_bytes(*client_ep.id().as_bytes()),
    )
    .await?;
    assert_eq!(resp.target, target_id, "target echoed");
    // The server's `serve()` refreshed the authenticated client
    // NodeId into its routing table BEFORE dispatching this request,
    // so the client itself appears in the response's `closer_nodes`
    // (modulo the server's own id, which `closest()` filters).
    assert_eq!(
        resp.closer_nodes.as_slice(),
        [NodeId::from_bytes(*client_ep.id().as_bytes())],
        "the freshly-refreshed client must appear as the only closer node"
    );
    // Server should have refreshed the authenticated client NodeId
    // into its routing table (PR 2 behaviour, unchanged by PR 4).
    let in_table = {
        let t = server.routing.lock().unwrap();
        t.contains(&NodeId::from_bytes(*client_ep.id().as_bytes()))
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
    staked.insert(NodeId::from_bytes(*server.id.as_bytes()));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
    let client_routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *client_id.as_bytes(),
    ))));

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
            NodeId::from_bytes(*server.id.as_bytes()),
            NodeId::from_bytes(*client_id.as_bytes()),
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
        assert!(t.contains(&NodeId::from_bytes(*server.id.as_bytes())));
    }
    // S's routing table now contains C — exercised by the bootstrap's
    // FindNode call against S.
    {
        let t = server.routing.lock().unwrap();
        assert!(t.contains(&NodeId::from_bytes(*client_id.as_bytes())));
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
    let hash = ContentHash::from_bytes([0x77u8; 32]);
    let ack = client::store(
        &client_ep,
        target,
        hash,
        NodeId::from_bytes(*client_id.as_bytes()),
    )
    .await?;
    assert!(ack.accepted, "staked publisher's Store must be accepted");
    assert_eq!(ack.hash, hash);

    // Server's RecordStore now has the record.
    {
        let r = server.records.lock().unwrap();
        assert_eq!(
            r.publisher_record_count(&NodeId::from_bytes(*client_id.as_bytes())),
            1
        );
        assert_eq!(r.len(), 1);
    }

    client_ep.close().await;
    server.endpoint.close().await;
    server.accept_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_republish_publishes_immediately_on_cache_insert() -> anyhow::Result<()> {
    // ADR 022 §STORE Flow steps 2-3: a freshly cached blob MUST be
    // published to the K+3 closest peers *immediately*, then
    // scheduled for next republish 30-50 min later. The previous
    // implementation scheduled with the steady-state window from the
    // start, so a blob wasn't discoverable for up to 50 minutes —
    // this test is the regression guard.
    use decdn_node::dht::{RepublishScheduler, publish::run_republish};
    use tokio::sync::{broadcast, oneshot};

    // Server S in `staked` so the publish-driven `Store` from the
    // republish task lands successfully.
    let publisher_sk = fresh_key();
    let publisher_id = publisher_sk.public();
    let mut staked = HashSet::new();
    staked.insert(*publisher_id.as_bytes());
    let server = spin_up_server(staked).await?;

    // Build the publisher endpoint + a routing table that seeds the
    // server as the only known peer (so the republish fan-out targets
    // it).
    let (publisher_ep, _) = local_endpoint(publisher_sk, vec![]).await?;
    let routing = Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
        *publisher_id.as_bytes(),
    ))));
    {
        let mut t = routing.lock().unwrap();
        t.insert(NodeId::from_bytes(*server.id.as_bytes()));
    }

    // Pre-resolve the server's path by issuing one `FindNode`
    // (loopback tests disable iroh discovery; without this the
    // republish task can't connect by NodeId alone).
    let target = EndpointAddr::new(server.id).with_ip_addr(server.addr);
    let _ = client::find_node(
        &publisher_ep,
        target,
        NodeId::from_bytes(*server.id.as_bytes()),
        NodeId::from_bytes(*publisher_id.as_bytes()),
    )
    .await?;

    // Spin up `run_republish` with a fake cache + insert channel.
    // We need a real `CacheEngine` for the stop-on-evict check; use
    // an empty cache rooted in a tempdir.
    let cache_dir = tempfile::tempdir()?;
    let cache = decdn_cache::CacheEngine::open(cache_dir.path(), vec![], 16).await?;
    let scheduler = Arc::new(RepublishScheduler::new());
    let (inserts_tx, inserts_rx) = broadcast::channel::<iroh_blobs::Hash>(16);
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(run_republish(
        publisher_ep.clone(),
        publisher_id,
        Arc::clone(&routing),
        Arc::clone(&scheduler),
        cache,
        true,
        Arc::new(decdn_node::metrics::Metrics::new()),
        inserts_rx,
        stop_rx,
    ));

    // Simulate a cache insert by broadcasting on the channel. The
    // republish task MUST send a `Store` to the server immediately;
    // we verify by checking the server's RecordStore picks it up.
    let hash = iroh_blobs::Hash::from_bytes([0xA5u8; 32]);
    inserts_tx.send(hash).unwrap();

    // Poll the server's record store with a generous timeout — the
    // republish task is async + needs one RTT.
    let mut got_record = false;
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let n = {
            let r = server.records.lock().unwrap();
            r.publisher_record_count(&NodeId::from_bytes(*publisher_id.as_bytes()))
        };
        if n >= 1 {
            got_record = true;
            break;
        }
    }
    let _ = stop_tx.send(());
    let _ = task.await;

    assert!(
        got_record,
        "republish task must send `Store` immediately on cache-insert; \
         server's RecordStore should hold the publisher's record within \
         1.5s but did not"
    );

    publisher_ep.close().await;
    server.endpoint.close().await;
    server.accept_task.abort();
    Ok(())
}

/// A lagged cache-commit channel must re-seed the scheduler from local state.
///
/// The channel is bounded and best-effort — the cache does not retain the
/// hashes it drops — so a blob committed inside the lag window would otherwise
/// carry no DHT record until an operator restarted the node. This drives the
/// real `run_republish` loop against a real `CacheEngine` and asserts the
/// missed hashes end up scheduled.
///
/// No server, and no peers in the routing table: the sweep's contract is what
/// reaches the *scheduler*. Publishing to peers is the tick path, covered by
/// `run_republish_publishes_immediately_on_cache_insert` above.
#[tokio::test(flavor = "multi_thread")]
async fn run_republish_lag_sweeps_the_cache_back_into_the_scheduler() -> anyhow::Result<()> {
    use decdn_node::dht::{RepublishScheduler, publish::run_republish};
    use tokio::sync::{broadcast, oneshot};

    let payloads: [&'static [u8]; 4] = [b"lag-a", b"lag-b", b"lag-c", b"lag-d"];
    let cache_dir = tempfile::tempdir()?;
    let origins: Vec<Arc<dyn decdn_cache::Origin>> =
        payloads.iter().map(|p| stub_origin(p)).collect();
    let cache = decdn_cache::CacheEngine::open(cache_dir.path(), origins, 16).await?;
    let hashes: Vec<iroh_blobs::Hash> = payloads
        .iter()
        .map(|p| decdn_cache::Hash::new(*p))
        .collect();
    for h in &hashes {
        cache.get(*h).await?;
    }

    // Overflow before the task starts polling: a receiver created ahead of
    // more sends than the channel holds sees `Lagged` on its first `recv`,
    // which makes the trigger deterministic rather than timing-dependent.
    let (inserts_tx, inserts_rx) = broadcast::channel::<iroh_blobs::Hash>(2);
    for h in &hashes {
        inserts_tx.send(*h).unwrap();
    }

    let key = fresh_key();
    let publisher_id = key.public();
    let (publisher_ep, _addr) = local_endpoint(key, vec![ALPN_DHT.to_vec()]).await?;
    let scheduler = Arc::new(RepublishScheduler::new());
    let metrics = Arc::new(Metrics::new());
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(run_republish(
        publisher_ep.clone(),
        publisher_id,
        // Empty routing table: with no peers the publish fan-out is a no-op,
        // which is what keeps this test about the sweep.
        Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *publisher_id.as_bytes(),
        )))),
        Arc::clone(&scheduler),
        cache,
        true,
        Arc::clone(&metrics),
        inserts_rx,
        stop_rx,
    ));

    // The sweep is detached and walks the store, so poll rather than assume
    // it has landed by any fixed point.
    let mut seeded_count = 0usize;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        seeded_count = scheduler.len();
        if seeded_count >= payloads.len() {
            break;
        }
    }
    let _ = stop_tx.send(());
    let _ = task.await;

    assert_eq!(
        seeded_count,
        payloads.len(),
        "the lag sweep must schedule every held blob, not just the ones the \
         receiver still saw after the overflow"
    );
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_lag_sweeps_total 1"),
        "the lag must surface on its counter:\n{text}"
    );
    assert!(
        text.lines()
            .any(|l| l == "decdn_dht_republish_seed_store_walk_failures_total 0"),
        "a healthy store walk must not count as a degraded seed:\n{text}"
    );

    publisher_ep.close().await;
    Ok(())
}

/// An origin-only node must not sweep in store-only content.
///
/// The sweep re-seeds through the same `relay_foreign_namespaces` gate the serve
/// path applies, so this pins the flag's threading from `run_republish` down to
/// the snapshot — an inverted or hardcoded value compiles and passes every other
/// test while making the node advertise blobs its serve gate then declines.
///
/// Both polarities run over identical fixtures so the assertion is a contrast,
/// not an absolute: the store-only blob is never sent on the insert channel, so
/// the only path that can schedule it is the sweep.
#[tokio::test(flavor = "multi_thread")]
async fn run_republish_lag_sweep_honours_the_origin_only_policy() -> anyhow::Result<()> {
    let origin_only = sweep_scheduled_count(false).await?;
    let relaying = sweep_scheduled_count(true).await?;

    assert_eq!(
        origin_only, 1,
        "an origin-only node must sweep in its origin-held content and nothing \
         from the store"
    );
    assert_eq!(
        relaying, 2,
        "a relaying node must sweep in both halves — otherwise the origin-only \
         assertion above proves nothing"
    );
    Ok(())
}

/// Drive one lag sweep at the given policy and return how many hashes ended up
/// scheduled. `own` is enumerable (origin-held) and is the only hash published
/// on the insert channel; `foreign` is fetch-only, so it lives in the store
/// alone and the sweep is the only path that can reach it.
async fn sweep_scheduled_count(relay_foreign_namespaces: bool) -> anyhow::Result<usize> {
    use decdn_node::dht::{RepublishScheduler, publish::run_republish};
    use tokio::sync::{broadcast, oneshot};

    let cache_dir = tempfile::tempdir()?;
    let own = decdn_cache::Hash::new(b"policy-own");
    let foreign = decdn_cache::Hash::new(b"policy-foreign");
    let cache = decdn_cache::CacheEngine::open(
        cache_dir.path(),
        vec![
            enumerable_stub_origin(b"policy-own"),
            stub_origin(b"policy-foreign"),
        ],
        16,
    )
    .await?;
    cache.get(own).await?;
    cache.get(foreign).await?;
    cache.rescan_origins().await;

    // Overflow before the receiver polls so `Lagged` is deterministic. Only
    // `own` rides the channel, so the eager per-insert arm can never be what
    // schedules `foreign`.
    let (inserts_tx, inserts_rx) = broadcast::channel::<iroh_blobs::Hash>(2);
    for _ in 0..4 {
        inserts_tx.send(own).unwrap();
    }

    let key = fresh_key();
    let publisher_id = key.public();
    let (publisher_ep, _addr) = local_endpoint(key, vec![ALPN_DHT.to_vec()]).await?;
    let scheduler = Arc::new(RepublishScheduler::new());
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(run_republish(
        publisher_ep.clone(),
        publisher_id,
        Arc::new(Mutex::new(RoutingTable::new(NodeId::from_bytes(
            *publisher_id.as_bytes(),
        )))),
        Arc::clone(&scheduler),
        cache,
        relay_foreign_namespaces,
        Arc::new(Metrics::new()),
        inserts_rx,
        stop_rx,
    ));

    // Poll up to the expected ceiling, then let the loop settle so a late
    // arrival would still be observed rather than raced past.
    let want = if relay_foreign_namespaces { 2 } else { 1 };
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if scheduler.len() >= want {
            break;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let settled = scheduler.len();
    let _ = stop_tx.send(());
    let _ = task.await;

    publisher_ep.close().await;
    Ok(settled)
}

/// [`stub_origin`]'s enumerable twin: also answers `enumerate` and `size`, which
/// is what puts the hash in the cache's origin-held index.
fn enumerable_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
    #[derive(Debug)]
    struct EnumerableStub {
        data: bytes::Bytes,
        hash: decdn_cache::Hash,
    }

    impl decdn_cache::Origin for EnumerableStub {
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
            let n = (hash == self.hash).then(|| u64::try_from(self.data.len()).unwrap_or(u64::MAX));
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
            let out = vec![self.hash];
            Box::pin(async move { Ok(out) })
        }
    }

    Arc::new(EnumerableStub {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
    })
}

/// Single-blob stub origin: `fetch` commits the payload into the store on
/// `get`, which is all this file needs to build a non-empty cache.
fn stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
    #[derive(Debug)]
    struct StubOrigin {
        data: bytes::Bytes,
        hash: decdn_cache::Hash,
    }

    impl decdn_cache::Origin for StubOrigin {
        fn kind(&self) -> decdn_cache::OriginKind {
            decdn_cache::OriginKind::Http
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
    }

    Arc::new(StubOrigin {
        data: bytes::Bytes::from_static(payload),
        hash: decdn_cache::Hash::new(payload),
    })
}
