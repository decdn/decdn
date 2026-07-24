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
//! 1. exchange `NodeAnnounce` gossip **mutually** — each node publishes its own
//!    announce and lands the other in its [`PeerTable`] (the real
//!    [`GossipService`] publisher + subscriber + `validate_envelope` +
//!    peer-table insert path, exercised in both directions), and
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
//! `subscribe_and_join(global_topic, [peer])` handle on **each** node's own
//! `Gossip` instance — A joins toward B and B joins toward A. Each join shares
//! the same per-`Gossip` topic actor as that node's `GossipService` subscriber,
//! so the join's dial pulls the node into the mesh and, once the membership
//! forms, broadcasts from *either* publisher reach *both* subscribers. Every
//! bootstrap peer address is resolved through the *same* `MemoryLookup`
//! discovery leg, so the mesh still forms with zero n0 infra. What this does
//! **not** cover: end-to-end mesh *self-bootstrap* by `GossipService` alone (it
//! has none), nor the live `PkarrPublisher` / `DnsAddressLookup` legs (those
//! need a real pkarr relay / DNS zone — see the "future work" note at the
//! bottom).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::CacheEngine;
use decdn_common::config::ResolvedSecurity;
use decdn_gossip::{
    AnnounceGate, AnnounceReject, GossipRuntimeConfig, GossipService, OwnedAnnounceGate, PeerTable,
    StakedNodeSet, build_gossip, metrics::NoopMetrics,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::probe::ProbeHandler;
use decdn_node::handlers::probe_rate_limit::ProbeRateLimiter;
use decdn_node::metrics::Metrics;
use decdn_node::rate_limit::RateLimitConfig;
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

/// Join background gossip tasks after shutdown without silently swallowing a
/// panic. A [`tokio::task::JoinError`] that is a panic is a real test failure
/// (e.g. a publisher or subscriber task panicked) and is re-raised here; a
/// clean cancel or a wind-down timeout are both tolerated.
async fn join_gossip_tasks(tasks: impl IntoIterator<Item = tokio::task::JoinHandle<()>>) {
    for h in tasks {
        match tokio::time::timeout(Duration::from_secs(2), h).await {
            Ok(Ok(())) => {}
            Ok(Err(join_err)) => {
                if let Ok(panic) = join_err.try_into_panic() {
                    std::panic::resume_unwind(panic);
                }
                // Otherwise the task was cancelled during shutdown — expected.
            }
            Err(_elapsed) => {
                // Task didn't wind down within the grace window; not a panic.
            }
        }
    }
}

/// Spawn a production [`GossipService`] for a node that subscribes to the global
/// topic and (because a `region` is set) runs the publisher too, so it both
/// emits its own `NodeAnnounce` and validates + inserts peers' announces.
#[allow(clippy::too_many_arguments)]
async fn spawn_publisher(
    ep: Endpoint,
    secret: SecretKey,
    gossip: iroh_gossip::net::Gossip,
    region: &str,
    peers: Arc<RwLock<PeerTable>>,
    metrics: Arc<dyn decdn_gossip::GossipMetrics>,
    shutdown: CancellationToken,
    gate: OwnedAnnounceGate,
) -> decdn_gossip::GossipHandles {
    GossipService::spawn(
        ep,
        secret,
        gossip,
        GossipRuntimeConfig {
            announce_interval_sec: 60,
            subscribe_global: true,
            region: Some(region.to_string()),
        },
        peers,
        metrics,
        shutdown,
        gate,
    )
    .await
    .expect("gossip service starts")
}

/// Accept-any stub for the ADR 001 rule-2 gate — stands in for a live registry
/// in which every participating node is currently staked.
#[derive(Debug)]
struct AllStaked;
impl StakedNodeSet for AllStaked {
    fn contains(&self, _node_id: &[u8; 32]) -> bool {
        true
    }
}

/// Reject-any stub for the ADR 001 rule-2 gate — stands in for a live registry
/// in which the announcing peer is *not* currently staked, so every inbound
/// `NodeAnnounce` must be dropped at the subscriber (`AnnounceReject::NotStaked`).
#[derive(Debug)]
struct NoneStaked;
impl StakedNodeSet for NoneStaked {
    fn contains(&self, _node_id: &[u8; 32]) -> bool {
        false
    }
}

/// A [`GossipMetrics`](decdn_gossip::GossipMetrics) that counts `not_staked`
/// rejects. `NoopMetrics` can't observe counters, so the reject test uses this
/// to prove the announce actually *reached* the subscriber's validator and was
/// dropped there (rather than being silently lost in the mesh). All other
/// counters are inert — only the rule-2 reject signal matters here.
#[derive(Debug, Default)]
struct NotStakedRejectCounter {
    not_staked: AtomicU64,
}
impl NotStakedRejectCounter {
    fn not_staked(&self) -> u64 {
        self.not_staked.load(Ordering::Relaxed)
    }
}
impl decdn_gossip::GossipMetrics for NotStakedRejectCounter {
    fn inc_rejected(&self, reason: &'static str) {
        if reason == AnnounceReject::NotStaked.label() {
            self.not_staked.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn inc_published(&self, _topic: &str) {}
    fn inc_received(&self, _topic: &str) {}
    fn set_peer_table_size(&self, _n: i64) {}
    fn inc_reconnected(&self, _topic: &str) {}
    fn add_evicted_ttl(&self, _n: u64) {}
}

/// Check whether `peers` has learned `node_id` via `NodeAnnounce`, asserting the
/// stored entry carries the expected node id and region. Returns `true` once the
/// peer is present (the announce arrived and was inserted).
async fn learned_peer(
    peers: &RwLock<PeerTable>,
    node_id: &[u8; 32],
    expected_region: &str,
) -> bool {
    let Some(entry) = peers.read().await.get(node_id).cloned() else {
        return false;
    };
    assert_eq!(
        &entry.announce.body.node_id, node_id,
        "stored announce node_id must match the awaited peer"
    );
    assert_eq!(
        entry.announce.body.region, expected_region,
        "stored announce must carry the region the peer published"
    );
    true
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

/// Permissive `ProbeRateLimiter` — this suite doesn't exercise the ADR 005
/// probe rate limiter, so all three layers are effectively unbounded.
fn permissive_probe_rate_limiter(metrics: &Arc<Metrics>) -> Arc<ProbeRateLimiter> {
    let cfg = RateLimitConfig {
        per_peer_rate_per_sec: 1e9,
        per_peer_burst: u32::MAX,
        per_ip_rate_per_sec: 1e9,
        per_ip_burst: u32::MAX,
        global_rate_per_sec: 1e9,
        global_burst: u32::MAX,
        max_tracked_per_ip: 4096,
        max_tracked_per_peer: 4096,
    };
    Arc::new(ProbeRateLimiter::new(&cfg, Arc::clone(metrics)))
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
///
/// Returns the cloned `MemoryLookup` handle too, so the caller can seed a peer
/// address out-of-band *after* binding (e.g. to teach a node that bound first
/// about a peer that bound later). The handle stays linked to the endpoint's
/// live lookup (`MemoryLookup` is `Arc`-backed), so a later
/// `add_endpoint_info` is visible to the running endpoint.
async fn bind_discovery_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
    peers: Vec<EndpointAddr>,
) -> anyhow::Result<(Endpoint, EndpointAddr, MemoryLookup)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let lookup = MemoryLookup::from_endpoint_info(peers);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key.clone())
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .address_lookup(lookup.clone())
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
    Ok((ep, addr, lookup))
}

/// Two nodes wired with ONLY operator-configured static-peer-map discovery
/// (`MemoryLookup` on `presets::Minimal`, relays disabled) exchange real
/// `NodeAnnounce`s **mutually** — each lands the other in its [`PeerTable`].
///
/// Both nodes carry a region (A "US", B "DE"), so each runs the production
/// [`GossipService`] publisher *and* subscriber: A publishes and B's subscriber
/// `validate_envelope`s + inserts A, and symmetrically B publishes and A's
/// subscriber inserts B. The mesh is seeded by one bootstrap-join handle per
/// node (A toward B, B toward A; see the module-level coverage caveat); every
/// bootstrap address resolves through that node's `MemoryLookup`, so no n0 infra
/// participates.
#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_exchange_node_announce_via_self_hosted_discovery() -> anyhow::Result<()> {
    let a_secret = SecretKey::generate();
    let b_secret = SecretKey::generate();
    let a_id = *a_secret.public().as_bytes();
    let b_id = *b_secret.public().as_bytes();

    // Bind A first so we know its addr; B's MemoryLookup is seeded with A at
    // bind time, then A's MemoryLookup is seeded with B once B binds. Both
    // directions resolve peers ONLY through these operator-configured
    // `MemoryLookup` legs. Both ALPNs include gossip.
    let (a_ep, a_addr, a_lookup) =
        bind_discovery_endpoint(a_secret.clone(), vec![GOSSIP_ALPN.to_vec()], Vec::new()).await?;
    let (b_ep, b_addr, _b_lookup) = bind_discovery_endpoint(
        b_secret.clone(),
        vec![GOSSIP_ALPN.to_vec()],
        // B learns A's address ONLY through this operator-configured leg.
        vec![a_addr.clone()],
    )
    .await?;
    // A learns B's address ONLY through this operator-configured leg, seeded
    // out-of-band now that B has bound and its addr is known.
    a_lookup.add_endpoint_info(b_addr.clone());

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

    // Node A: publisher with region "US" -> emits NodeAnnounce on global+region
    // and runs the subscriber that validates + inserts B's announce. Node B is
    // wired identically with region "DE"; giving B a region (mirroring A)
    // enables its publisher, so the exchange is mutual.
    // Both nodes staked: the real spawn → subscriber → `validate_envelope`
    // path enforces ADR 001 rule 2, and a mutual learn proves the gate is
    // threaded end-to-end and admits staked announcers.
    let a_handles = spawn_publisher(
        a_ep.clone(),
        a_secret.clone(),
        a_gossip.clone(),
        "US",
        Arc::clone(&a_peers),
        Arc::clone(&metrics),
        shutdown.clone(),
        AnnounceGate::Enforce(Arc::new(AllStaked)),
    )
    .await;
    let b_handles = spawn_publisher(
        b_ep.clone(),
        b_secret.clone(),
        b_gossip.clone(),
        "DE",
        Arc::clone(&b_peers),
        Arc::clone(&metrics),
        shutdown.clone(),
        AnnounceGate::Enforce(Arc::new(AllStaked)),
    )
    .await;

    // Seed the mesh in both directions: each node opens one bootstrap-join on
    // the global topic toward the other. A node's `MemoryLookup` resolves the
    // bootstrap NodeId -> 127.0.0.1:port, so every dial uses operator discovery
    // exclusively. Each join shares that node's per-Gossip topic actor with its
    // GossipService subscriber, so once the membership forms, broadcasts from
    // either publisher reach both subscribers. Held to keep the membership
    // alive. (See the module-level coverage caveat: GossipService has no
    // self-bootstrap, so this seeding stands in for the out-of-band swarm seed a
    // real deployment provides.)
    let _a_bootstrap = a_gossip
        .subscribe_and_join(global_topic_id(), vec![b_secret.public()])
        .await
        .map_err(|e| anyhow::anyhow!("A bootstrap subscribe_and_join: {e}"))?;
    let _b_bootstrap = b_gossip
        .subscribe_and_join(global_topic_id(), vec![a_secret.public()])
        .await
        .map_err(|e| anyhow::anyhow!("B bootstrap subscribe_and_join: {e}"))?;

    // Fire immediate announces from both nodes rather than waiting the 60s
    // interval. Both publishers are wired because both nodes carry a region.
    let a_trigger = a_handles
        .announce_trigger
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("node A publisher should be wired (region set)"))?;
    let b_trigger = b_handles
        .announce_trigger
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("node B publisher should be wired (region set)"))?;

    // Poll: drive a fresh announce from each node every round and check that B
    // learned A *and* A learned B. The mesh takes a few hundred ms to form
    // (dial + HyParView handshake + first broadcast); bound generously so a slow
    // CI doesn't flake, but the happy path resolves in well under a second.
    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..50 {
        a_trigger.announce_now();
        b_trigger.announce_now();
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !saw_a {
            saw_a = learned_peer(&b_peers, &a_id, "US").await;
        }
        if !saw_b {
            saw_b = learned_peer(&a_peers, &b_id, "DE").await;
        }
        if saw_a && saw_b {
            break;
        }
    }
    assert!(
        saw_a,
        "node B must learn node A via NodeAnnounce over self-hosted discovery \
         (MemoryLookup only, no n0 DNS/pkarr/relay)"
    );
    assert!(
        saw_b,
        "node A must learn node B via NodeAnnounce over self-hosted discovery \
         (MemoryLookup only, no n0 DNS/pkarr/relay) — proving MUTUAL insertion"
    );

    // Teardown. Join the background gossip tasks, re-raising any task panic
    // rather than dropping it (see `join_gossip_tasks`).
    shutdown.cancel();
    join_gossip_tasks(a_handles.tasks.into_iter().chain(b_handles.tasks)).await;
    a_router.shutdown().await.ok();
    b_router.shutdown().await.ok();
    a_ep.close().await;
    b_ep.close().await;
    Ok(())
}

/// Rule-2 **reject** direction (#1222): a subscriber whose gate excludes the
/// publisher drops the announce and never inserts it. This complements
/// `two_nodes_exchange_node_announce_via_self_hosted_discovery`, which wires both
/// sides with `AllStaked` and only exercises the *accept* direction. Here B's
/// gate is `NoneStaked`, so A's announce must be rejected on B's real
/// `spawn → subscriber_task → validate_envelope` path with
/// `AnnounceReject::NotStaked`, leaving B's peer table empty. B runs a counting
/// metric so the test proves the reject actually *fired* — i.e. A's announce
/// reached B's validator and was dropped — rather than the weaker "the announce
/// never showed up" (which an unformed mesh would also satisfy).
#[tokio::test(flavor = "multi_thread")]
async fn non_staked_announce_is_dropped_at_subscriber() -> anyhow::Result<()> {
    let a_secret = SecretKey::generate();
    let b_secret = SecretKey::generate();
    let a_id = *a_secret.public().as_bytes();

    // Same self-hosted discovery wiring as the accept test: MemoryLookup only,
    // no n0 DNS/pkarr/relay. Bind A first, seed B with A, then seed A with B.
    let (a_ep, a_addr, a_lookup) =
        bind_discovery_endpoint(a_secret.clone(), vec![GOSSIP_ALPN.to_vec()], Vec::new()).await?;
    let (b_ep, b_addr, _b_lookup) = bind_discovery_endpoint(
        b_secret.clone(),
        vec![GOSSIP_ALPN.to_vec()],
        vec![a_addr.clone()],
    )
    .await?;
    a_lookup.add_endpoint_info(b_addr.clone());

    let a_gossip = build_gossip(a_ep.clone());
    let b_gossip = build_gossip(b_ep.clone());

    let a_router = Router::builder(a_ep.clone())
        .accept(GOSSIP_ALPN, a_gossip.clone())
        .spawn();
    let b_router = Router::builder(b_ep.clone())
        .accept(GOSSIP_ALPN, b_gossip.clone())
        .spawn();

    let a_peers = Arc::new(RwLock::new(PeerTable::new(60_000_000, 128)));
    let b_peers = Arc::new(RwLock::new(PeerTable::new(60_000_000, 128)));
    let a_metrics: Arc<dyn decdn_gossip::GossipMetrics> = Arc::new(NoopMetrics);
    // Keep a typed handle to read the reject counter after the mesh has run;
    // hand the subscriber a trait-object clone of the same counter.
    let b_metrics = Arc::new(NotStakedRejectCounter::default());
    let b_metrics_gossip: Arc<dyn decdn_gossip::GossipMetrics> = b_metrics.clone();
    let shutdown = CancellationToken::new();

    // A: staked publisher. Its own gate is irrelevant to the reject direction —
    // what matters is that A actually broadcasts a `NodeAnnounce`.
    let a_handles = spawn_publisher(
        a_ep.clone(),
        a_secret.clone(),
        a_gossip.clone(),
        "US",
        Arc::clone(&a_peers),
        Arc::clone(&a_metrics),
        shutdown.clone(),
        AnnounceGate::Enforce(Arc::new(AllStaked)),
    )
    .await;
    // B: receiver whose gate EXCLUDES A, so B must reject every A announce.
    let b_handles = spawn_publisher(
        b_ep.clone(),
        b_secret.clone(),
        b_gossip.clone(),
        "DE",
        Arc::clone(&b_peers),
        b_metrics_gossip,
        shutdown.clone(),
        AnnounceGate::Enforce(Arc::new(NoneStaked)),
    )
    .await;

    // Seed the mesh in both directions (same rationale as the accept test) so
    // A's broadcast reaches B's subscriber. Held to keep membership alive.
    let _a_bootstrap = a_gossip
        .subscribe_and_join(global_topic_id(), vec![b_secret.public()])
        .await
        .map_err(|e| anyhow::anyhow!("A bootstrap subscribe_and_join: {e}"))?;
    let _b_bootstrap = b_gossip
        .subscribe_and_join(global_topic_id(), vec![a_secret.public()])
        .await
        .map_err(|e| anyhow::anyhow!("B bootstrap subscribe_and_join: {e}"))?;

    let a_trigger = a_handles
        .announce_trigger
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("node A publisher should be wired (region set)"))?;
    let b_trigger = b_handles
        .announce_trigger
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("node B publisher should be wired (region set)"))?;

    // Drive a fresh announce from each node every round. Two invariants hold on
    // every iteration until B's validator rejects A: B must NEVER insert A, and
    // once the mesh forms B must record a `not_staked` reject. Bound generously
    // (same 50×100ms budget as the accept test) so slow CI doesn't flake.
    let mut rejected = false;
    for _ in 0..50 {
        a_trigger.announce_now();
        b_trigger.announce_now();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !learned_peer(&b_peers, &a_id, "US").await,
            "receiver B must never insert the non-staked publisher A (rule-2 reject)"
        );
        // `not_staked > 0` attributes the reject to A only because (a) there are
        // exactly two nodes and (b) plumtree never routes a message back to its
        // origin, so B never receives — and rejects — its own announce. If it
        // ever did, `NoneStaked` would reject B's self-echo inside
        // `validate_envelope` (the self-echo drop is in the later `Ok` arm, after
        // validation), satisfying this check without A's announce arriving. Safe
        // today; revisit if either assumption changes.
        if b_metrics.not_staked() > 0 {
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "B's subscriber must reject A's announce with not_staked — proving the \
         announce reached B's validator and was dropped, not silently lost in the mesh"
    );
    // End-state invariant, made explicit at teardown and as a guard against
    // future edits to the loop's break condition. Not a race fix: `NoneStaked`
    // rejects unconditionally, so no A announce can ever be admitted — but
    // asserting it here documents that the reject state holds right up to shutdown.
    assert!(
        !learned_peer(&b_peers, &a_id, "US").await,
        "receiver B must still not have inserted the non-staked publisher A after the reject"
    );

    shutdown.cancel();
    join_gossip_tasks(a_handles.tasks.into_iter().chain(b_handles.tasks)).await;
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
    let (server_ep, server_addr, _server_lookup) =
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
        permissive_probe_rate_limiter(&metrics),
        cache,
        Arc::clone(&signer),
        domain.clone(),
        decdn_node::rate_bounds::RateBounds::new(0, MAX_RATE_PER_MB),
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
    let (client_ep, _client_addr, _client_lookup) =
        bind_discovery_endpoint(SecretKey::generate(), vec![], vec![server_addr.clone()]).await?;

    // Connect by NodeId ONLY — no `.with_ip_addr(...)`. iroh must resolve the
    // address through `MemoryLookup`. On `presets::Minimal` with no relay there
    // is no n0 DNS/pkarr to fall back to, so a successful connect can only mean
    // self-hosted discovery resolved it.
    let target = EndpointAddr::new(server_id);
    let conn = tokio::time::timeout(
        Duration::from_secs(5),
        client_ep.connect(target, ALPN_PROBE),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connect timed out after 5s (discovery never resolved?)"))?
    .map_err(|e| anyhow::anyhow!("connect (NodeId-only, discovery-resolved): {e}"))?;

    let (mut send, mut recv) = tokio::time::timeout(Duration::from_secs(5), conn.open_bi())
        .await
        .map_err(|_| anyhow::anyhow!("open_bi timed out after 5s"))?
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let req = ProbeRequest {
        hash: [0x5au8; 32],
        timestamp_us: 0x00c0_ffee,
    };
    write_frame(&mut send, &encode_message(&ProbeMessage::Request(req))?)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    let frame = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut recv))
        .await
        .map_err(|_| anyhow::anyhow!("read_frame timed out after 5s"))?
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
