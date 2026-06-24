//! Two-node integration coverage for operator-configured self-hosted
//! discovery (#967, exercising the discovery seam added in #829).
//!
//! # What this proves
//!
//! #829 lets an operator replace the n0-hosted DNS/pkarr address-lookup leg
//! with their own discovery providers, so a network can be fully
//! n0-independent. The fully-offline variant is the **static peer map**
//! (`[network.discovery.peers.<NodeId>]`), which `runtime::add_discovery_lookups`
//! wires into an iroh [`MemoryLookup`] on a `presets::Minimal` base — i.e. NO
//! n0 DNS, NO n0 pkarr, and (with relays disabled here) NO n0 relay. This
//! suite stands up two nodes wired **exactly** that way and shows they:
//!
//! 1. exchange `NodeAnnounce` gossip and land each other in their
//!    [`PeerTable`]s (the real [`GossipService`] publisher + subscriber +
//!    [`validate_envelope`] + peer-table insert path), and
//! 2. complete a `cdn/probe/v1` round-trip where the **client resolves the
//!    server by `NodeId` alone** — the address is supplied only by the
//!    operator-configured `MemoryLookup`, so a successful connect is direct
//!    proof that operator discovery (not n0 infra) resolved the address.
//!
//! # Why there is no n0 fallback anywhere here
//!
//! Every endpoint in this file is built on `Endpoint::builder(presets::Minimal)`
//! with `RelayMode::Disabled` and *only* a `MemoryLookup` address-lookup leg —
//! the same composition `runtime::add_discovery_lookups` produces for a
//! static-peer-map config. `presets::Minimal` installs no discovery service of
//! its own (unlike `presets::N0`), so if `MemoryLookup` did not resolve the
//! peer there would be no other way to find it and the connect/announce would
//! fail. The tests are therefore self-certifying: they cannot pass via a
//! silent n0 DNS/pkarr/relay fallback because no such service is wired. Nothing
//! in this file touches the network beyond loopback.
//!
//! # Coverage caveat (read before strengthening)
//!
//! `decdn_gossip::GossipService::spawn` subscribes every topic with an **empty**
//! iroh-gossip bootstrap set (`gossip.subscribe(topic_id, Vec::new())`). With no
//! bootstrap peer and no relay, iroh-gossip's `HyParView` membership never
//! proactively dials anyone — a node can only *accept* inbound gossip
//! connections, never originate the mesh. In a real deployment the gossip swarm
//! is seeded out-of-band (DHT bootstrap dials, operator-provided peers, etc.);
//! that seeding is **not** part of `GossipService` and is out of scope for #967.
//!
//! So this test seeds the two-node mesh deterministically by opening one extra
//! `subscribe_and_join(global_topic, [peer])` handle on the receiver's own
//! `Gossip` instance — sharing the same per-`Gossip` topic actor as its
//! `GossipService` subscriber, the join's dial pulls the receiver into the mesh
//! with the publisher. The bootstrap peer's address is resolved through the
//! *same* `MemoryLookup` discovery leg, so the mesh still forms with zero n0
//! infra. What this does **not** cover: end-to-end mesh *self-bootstrap* by
//! `GossipService` alone (it has none), nor the live `PkarrPublisher` /
//! `DnsAddressLookup` legs (those need a real pkarr relay / DNS zone — see the
//! "future work" note at the bottom).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::CacheEngine;
use decdn_common::config::ResolvedSecurity;
use decdn_gossip::{
    GossipRuntimeConfig, GossipService, PeerTable, ReputationWiring, build_gossip,
    metrics::NoopMetrics,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::probe::ProbeHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::{
    ALPN_PROBE, MAX_RATE_PER_MB, ProbeMessage, TOPIC_GLOBAL, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse},
    read_frame, write_frame,
};
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::{ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use iroh_gossip::ALPN as GOSSIP_ALPN;
use iroh_gossip::proto::TopicId;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// iroh-gossip topic id for the global `NodeAnnounce` topic. Mirrors
/// `decdn_gossip::service::topic_id` (private): the blake3 hash of the topic
/// name, matching iroh-gossip's own convention. Reconstructed here so the test
/// can open a bootstrap-join handle on the same topic the `GossipService`
/// subscriber listens on.
fn global_topic_id() -> TopicId {
    TopicId::from_bytes(*blake3::hash(TOPIC_GLOBAL.as_bytes()).as_bytes())
}

fn test_slash_domain() -> Eip712Domain {
    decdn_incentive::slash_judge_domain(421_614, Address::repeat_byte(0x11))
}

async fn empty_cache() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), vec![], 16).await?;
    Ok((cache, tmp))
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

/// Bind an iroh endpoint wired the way `runtime::add_discovery_lookups`
/// composes a **static-peer-map** discovery config: `presets::Minimal` (no n0
/// discovery service), `RelayMode::Disabled` (no n0 relay), and a single
/// `MemoryLookup` address-lookup leg seeded with `peers`. Binds to
/// `127.0.0.1:0` and returns the live endpoint plus its `127.0.0.1` addr.
///
/// This is the production discovery seam reduced to its offline core: if
/// `MemoryLookup` cannot resolve a peer, nothing else can — there is no n0
/// fallback to mask a discovery failure.
async fn bind_discovery_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
    peers: Vec<EndpointAddr>,
) -> anyhow::Result<(Endpoint, EndpointAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key.clone())
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .address_lookup(MemoryLookup::from_endpoint_info(peers))
        .bind_addr(bind)
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind: {e}"))?;
    let bound = ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let socket = match bound {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };
    let addr = EndpointAddr::new(secret_key.public()).with_ip_addr(socket);
    Ok((ep, addr))
}

/// Two nodes wired with ONLY operator-configured static-peer-map discovery
/// (`MemoryLookup` on `presets::Minimal`, relays disabled) exchange a real
/// `NodeAnnounce` and each lands the other in its [`PeerTable`].
///
/// The publisher (node A, region "US") runs the production [`GossipService`]
/// publisher; the receiver (node B, subscribe-only on the global topic) runs
/// the production subscriber, which `validate_envelope`s the announce and
/// inserts it. The mesh is seeded by one extra bootstrap-join handle on B (see
/// the module-level coverage caveat); the bootstrap address is resolved through
/// B's `MemoryLookup`, so no n0 infra participates.
#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_exchange_node_announce_via_self_hosted_discovery() -> anyhow::Result<()> {
    let a_secret = SecretKey::generate();
    let b_secret = SecretKey::generate();
    let a_id = *a_secret.public().as_bytes();

    // Bind A first so we know its addr; B's MemoryLookup is seeded with A, and
    // A's MemoryLookup is seeded with B once B binds. Both ALPNs include gossip.
    let (a_ep, a_addr) =
        bind_discovery_endpoint(a_secret.clone(), vec![GOSSIP_ALPN.to_vec()], Vec::new()).await?;
    let (b_ep, _b_addr) = bind_discovery_endpoint(
        b_secret.clone(),
        vec![GOSSIP_ALPN.to_vec()],
        // B learns A's address ONLY through this operator-configured leg.
        vec![a_addr.clone()],
    )
    .await?;

    let a_gossip = build_gossip(a_ep.clone());
    let b_gossip = build_gossip(b_ep.clone());

    // Both nodes serve the gossip ALPN so an inbound dial is accepted and the
    // neighbour relationship becomes bidirectional.
    let a_router = Router::builder(a_ep.clone())
        .accept(GOSSIP_ALPN, a_gossip.clone())
        .spawn();
    let b_router = Router::builder(b_ep.clone())
        .accept(GOSSIP_ALPN, b_gossip.clone())
        .spawn();

    let a_peers = Arc::new(RwLock::new(PeerTable::new(60_000_000, 128)));
    let b_peers = Arc::new(RwLock::new(PeerTable::new(60_000_000, 128)));
    let metrics: Arc<dyn decdn_gossip::GossipMetrics> = Arc::new(NoopMetrics);
    let shutdown = CancellationToken::new();

    // Node A: publisher with region "US" -> emits NodeAnnounce on global+region.
    let a_handles = GossipService::spawn(
        a_ep.clone(),
        a_secret.clone(),
        a_gossip.clone(),
        GossipRuntimeConfig {
            announce_interval_sec: 60,
            subscribe_global: true,
            region: Some("US".to_string()),
            allowlist: std::collections::HashSet::new(),
            subscribe_reputation: false,
            reputation_publish_interval_sec: 3600,
        },
        Arc::clone(&a_peers),
        Arc::clone(&metrics),
        shutdown.clone(),
        ReputationWiring::default(),
    )
    .await
    .expect("node A gossip service starts");

    // Node B: subscribe-only on the global topic (no region -> publisher off).
    // This is the production subscriber path that validates + inserts.
    let b_handles = GossipService::spawn(
        b_ep.clone(),
        b_secret.clone(),
        b_gossip.clone(),
        GossipRuntimeConfig {
            announce_interval_sec: 60,
            subscribe_global: true,
            region: None,
            allowlist: std::collections::HashSet::new(),
            subscribe_reputation: false,
            reputation_publish_interval_sec: 3600,
        },
        Arc::clone(&b_peers),
        Arc::clone(&metrics),
        shutdown.clone(),
        ReputationWiring::default(),
    )
    .await
    .expect("node B gossip service starts");

    // Seed the mesh: B joins the global topic with A as bootstrap. B's
    // MemoryLookup resolves A's NodeId -> 127.0.0.1:port, so the dial uses
    // operator discovery exclusively. This handle shares B's per-Gossip topic
    // actor with B's GossipService subscriber, so once the mesh forms, A's
    // broadcasts reach that subscriber. Held to keep the membership alive.
    let _b_bootstrap = b_gossip
        .subscribe_and_join(global_topic_id(), vec![a_secret.public()])
        .await
        .map_err(|e| anyhow::anyhow!("B bootstrap subscribe_and_join: {e}"))?;

    // Fire immediate announces from A rather than waiting the 60s interval.
    let a_trigger = a_handles
        .announce_trigger
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("node A publisher should be wired (region set)"))?;

    // Poll: drive a fresh announce each round and check B's peer table. The
    // mesh takes a few hundred ms to form (dial + HyParView handshake +
    // first broadcast); bound generously so a slow CI doesn't flake, but the
    // happy path resolves in well under a second.
    let mut saw_a = false;
    for _ in 0..50 {
        a_trigger.announce_now();
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(entry) = b_peers.read().await.get(&a_id) {
            assert_eq!(
                entry.announce.body.node_id, a_id,
                "stored announce must be A's"
            );
            assert_eq!(
                entry.announce.body.region, "US",
                "A announced region US; the stored entry must carry it"
            );
            saw_a = true;
            break;
        }
    }
    assert!(
        saw_a,
        "node B must learn node A via NodeAnnounce over self-hosted discovery \
         (MemoryLookup only, no n0 DNS/pkarr/relay)"
    );

    // Teardown.
    shutdown.cancel();
    for h in a_handles.tasks.into_iter().chain(b_handles.tasks) {
        let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
    }
    a_router.shutdown().await.ok();
    b_router.shutdown().await.ok();
    a_ep.close().await;
    b_ep.close().await;
    Ok(())
}

/// A `cdn/probe/v1` round-trip where the client resolves the server **by
/// `NodeId` alone**, with the address supplied only by the operator-configured
/// `MemoryLookup`. A successful response is direct proof that self-hosted
/// discovery (not n0 DNS/pkarr) resolved the address: both endpoints are
/// `presets::Minimal` with relays disabled, so there is no other resolver.
#[tokio::test(flavor = "multi_thread")]
async fn probe_roundtrip_resolves_server_by_node_id_via_self_hosted_discovery() -> anyhow::Result<()>
{
    let rate_per_mb: u64 = 42;

    let server_secret = SecretKey::generate();
    let server_id = server_secret.public();

    // Server endpoint: discovery is irrelevant for *accepting*, but we keep the
    // same Minimal + relays-disabled + empty-MemoryLookup composition so the
    // server, too, is provably free of n0 infra.
    let (server_ep, server_addr) =
        bind_discovery_endpoint(server_secret.clone(), vec![ALPN_PROBE.to_vec()], Vec::new())
            .await?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (cache, _cache_tmp) = empty_cache().await?;
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        Arc::new(AtomicU64::new(rate_per_mb)),
        Arc::clone(&metrics),
        limiter,
        cache,
        Arc::clone(&signer),
        domain.clone(),
        0,
        MAX_RATE_PER_MB,
        false,
        None,
    ));

    let server_ep_bg = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        if let Some(incoming) = server_ep_bg.accept().await {
            let connecting = incoming
                .accept()
                .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
            let conn = connecting
                .await
                .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
            handler
                .accept(conn)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        Ok::<_, anyhow::Error>(())
    });

    // Client endpoint: its ONLY way to find the server's address is the
    // operator-configured MemoryLookup seeded with the server's EndpointAddr.
    let (client_ep, _client_addr) =
        bind_discovery_endpoint(SecretKey::generate(), vec![], vec![server_addr.clone()]).await?;

    // Connect by NodeId ONLY — no `.with_ip_addr(...)`. iroh must resolve the
    // address through `MemoryLookup`. On `presets::Minimal` with no relay there
    // is no n0 DNS/pkarr to fall back to, so a successful connect can only mean
    // self-hosted discovery resolved it.
    let target = EndpointAddr::new(server_id);
    let conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect (NodeId-only, discovery-resolved): {e}"))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let req = ProbeRequest {
        hash: [0x5au8; 32],
        timestamp_us: 0x00c0_ffee,
    };
    write_frame(&mut send, &encode_message(&ProbeMessage::Request(req))?)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read: {e}"))?;
    let (msg, _rest) = decode_message::<ProbeMessage>(&frame)?;
    let resp: ProbeResponse = match msg {
        ProbeMessage::Response(r) => r,
        ProbeMessage::Request(_) => anyhow::bail!("unexpected request variant on client"),
    };

    assert_eq!(resp.body.timestamp_us, req.timestamp_us, "timestamp echoed");
    assert_eq!(resp.body.hash, req.hash, "hash echoed");
    assert_eq!(resp.body.rate_per_mb, rate_per_mb, "rate echoed");
    assert!(!resp.body.has_blob, "empty cache reports has_blob=false");

    conn.close(0u32.into(), b"bye");
    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

// Future work (out of scope for #967, needs infra the test can't stand up
// hermetically): exercise the live `PkarrPublisher` + `DnsAddressLookup` legs
// against an in-test pkarr relay / DNS zone, and exercise `GossipService` mesh
// self-bootstrap once it gains an operator-bootstrap-peer seam. Both are noted
// in the module docs above so the coverage boundary is explicit, not silent.
