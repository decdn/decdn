//! Warm-reconnect coverage for `cdn/probe/v1`.
//!
//! A client that already holds a resumable TLS session for a node re-probes it
//! over a plain QUIC handshake: resumption saves certificate transmission and
//! signature verification (the ECDHE exchange still runs), and no application
//! bytes ride along with the `ClientHello`. This suite drives the production
//! [`ProbeHandler`] with both probe clients over one long-lived client
//! endpoint, and asserts the answer is invariant across repeat probes and that
//! serving real probe traffic produces no early-data or session-ticket
//! accounting.
//!
//! **What is asserted, and what is only arranged.** Reusing one client
//! endpoint is the warm-reconnect *shape*: the `rustls` session cache lives on
//! the endpoint, so probes after the first have a ticket available. Whether a
//! given handshake actually resumed is not asserted, because iroh exposes no
//! way to ask — `Connection::handshake_data` carries ALPN and server name
//! only, and `ConnectionStats` has no resumption field. So these tests would
//! also pass if resumption silently stopped working; they pin the answer, not
//! the handshake path. Treat that as a known gap, not an oversight.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::CacheEngine;
use decdn_common::config::ResolvedSecurity;
use decdn_incentive::ProbeSlashData;
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::probe::ProbeHandler;
use decdn_node::handlers::probe_rate_limit::ProbeRateLimiter;
use decdn_node::metrics::Metrics;
use decdn_node::rate_limit::RateLimitConfig;
use decdn_protocol::{ALPN_PROBE, SLASH_SIG_LEN, message::ProbeResponse};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

/// Rate the server quotes; echoed back unclamped by the permissive bounds.
const RATE_PER_MB: u64 = 42;
/// Hash the probes ask about. The server runs an empty cache, so every answer
/// is `has_blob: false` and the hash only has to round-trip.
const PROBE_HASH: [u8; 32] = [0x5au8; 32];
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Slack for the endpoint driver to finish ingesting the server's
/// `NewSessionTicket`.
///
/// In practice the ticket arrives during the probe exchange and is cached well
/// before `probe_once` returns — which closes the connection — so this is
/// belt-and-braces for a ticket still in flight at close, not the thing that
/// makes the next probe warm. Shortening it should not make anything flaky.
const TICKET_SETTLE: Duration = Duration::from_millis(500);

fn test_slash_domain() -> Eip712Domain {
    decdn_incentive::slash_judge_domain(421_614, Address::repeat_byte(0x11))
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

/// Unbounded `ProbeRateLimiter` — this suite exercises the transport, not the
/// ADR 005 rate-limit layers.
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

/// Loopback endpoint, relays disabled. The TLS ticket cache is iroh's default:
/// 1-RTT session resumption is exactly what a warm reconnection uses.
async fn local_endpoint(sk: SecretKey) -> anyhow::Result<(Endpoint, SocketAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(sk)
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

/// Production probe server: an iroh `Router` serving [`ProbeHandler`] over an
/// empty cache. The `TempDir` guard must outlive the server.
///
/// Takes the `SecretKey` so a caller can respawn under the same node identity
/// on a fresh port — what a restarted operator looks like to a client holding
/// stale TLS session state.
async fn spawn_probe_server(
    metrics: &Arc<Metrics>,
    sk: SecretKey,
) -> anyhow::Result<(
    Router,
    Endpoint,
    EndpointAddr,
    Arc<PrivateKeySigner>,
    Eip712Domain,
    tempfile::TempDir,
)> {
    let cache_tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(cache_tmp.path(), vec![], 16).await?;
    let id = sk.public();
    let (ep, addr) = local_endpoint(sk).await?;
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let handler = Arc::new(ProbeHandler::new(
        id,
        RATE_PER_MB,
        Arc::clone(metrics),
        permissive_limiter(metrics),
        permissive_probe_rate_limiter(metrics),
        cache,
        Arc::clone(&signer),
        domain.clone(),
        decdn_node::rate_bounds::RateBounds::new(0),
        None, // no stake-lane reservation (#757)
        true, // relay foreign namespaces (default; #1759)
    ));
    let router = Router::builder(ep.clone())
        .accept(ALPN_PROBE, handler)
        .spawn();
    Ok((
        router,
        ep,
        EndpointAddr::new(id).with_ip_addr(addr),
        signer,
        domain,
        cache_tmp,
    ))
}

/// Assert the answer echoes the request and carries a `slash_sig` that
/// recovers to `signer` (ADR 005 / ADR 014).
fn assert_answer(
    resp: &ProbeResponse,
    timestamp_us: u64,
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
) -> anyhow::Result<()> {
    anyhow::ensure!(resp.body.hash == PROBE_HASH, "hash must be echoed");
    anyhow::ensure!(
        resp.body.timestamp_us == timestamp_us,
        "timestamp must be echoed"
    );
    anyhow::ensure!(
        resp.body.rate_per_mb == RATE_PER_MB,
        "quoted rate must survive the clamp"
    );
    anyhow::ensure!(
        !resp.body.has_blob,
        "empty cache must answer has_blob=false"
    );
    anyhow::ensure!(
        resp.slash_sig.len() == SLASH_SIG_LEN,
        "slash_sig must be {SLASH_SIG_LEN} bytes, got {}",
        resp.slash_sig.len()
    );
    let sig = Signature::try_from(resp.slash_sig.as_slice())
        .map_err(|e| anyhow::anyhow!("slash_sig parse: {e}"))?;
    ProbeSlashData {
        hash: B256::from(resp.body.hash),
        has_blob: resp.body.has_blob,
        rate_per_mb: resp.body.rate_per_mb,
        timestamp_us: resp.body.timestamp_us,
    }
    .verify_signer(&sig, signer.address(), domain)
    .map_err(|e| anyhow::anyhow!("slash_sig verify: {e}"))?;
    Ok(())
}

fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
    let needle = format!("{name} {value}");
    text.lines().any(|l| l == needle)
}

/// Cold probe, then two more over the same client endpoint — one through each
/// probe client. All three must answer identically, and the node must account
/// for exactly three probes with no early-data or session-ticket series in its
/// exposition.
///
/// Named for what it proves: reusing the endpoint is the warm-reconnect setup,
/// but resumption itself is not observable — see the module doc.
#[tokio::test(flavor = "multi_thread")]
async fn repeat_probes_on_a_reused_endpoint_answer_like_the_first() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (router, server_ep, target, signer, domain, _cache_tmp) =
        spawn_probe_server(&metrics, SecretKey::generate()).await?;

    // One long-lived client endpoint: what makes the later probes resumable.
    let (client, _client_addr) = local_endpoint(SecretKey::generate()).await?;

    // Cold: nothing cached for this peer yet.
    let (cold, _rtt) = decdn_client_pull::probe::probe_once(
        &client,
        target.clone(),
        PROBE_HASH,
        0x1001,
        PROBE_TIMEOUT,
    )
    .await?;
    assert_answer(&cold, 0x1001, &signer, &domain)?;

    // Let the server's session ticket land so the next probes are warm.
    tokio::time::sleep(TICKET_SETTLE).await;

    let (warm, _rtt) = decdn_client_pull::probe::probe_once(
        &client,
        target.clone(),
        PROBE_HASH,
        0x1002,
        PROBE_TIMEOUT,
    )
    .await?;
    assert_answer(&warm, 0x1002, &signer, &domain)?;
    anyhow::ensure!(
        warm.body.rate_per_mb == cold.body.rate_per_mb && warm.body.has_blob == cold.body.has_blob,
        "a warm reconnection must not change the answer"
    );

    // The daemon-side copy of the client, on the same warm endpoint.
    let (warm_node, _rtt) = decdn_node::client_requester::probe::probe_once(
        &client,
        target.clone(),
        PROBE_HASH,
        0x1003,
        PROBE_TIMEOUT,
    )
    .await?;
    assert_answer(&warm_node, 0x1003, &signer, &domain)?;

    // Shut the router down *before* scraping: the handler increments
    // `decdn_probe_requests_total` after writing the response frame, so a
    // client that has already returned can still be ahead of the server task.
    client.close().await;
    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router shutdown: {e}"))?;
    server_ep.close().await;

    let text = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    anyhow::ensure!(
        has_metric_line(&text, "decdn_probe_requests_total", 3),
        "server must have served three probes:\n{text}"
    );
    // Real probe traffic, reused connections included, must not move any
    // early-data or session-ticket accounting: none exists.
    for needle in ["0rtt", "session_ticket"] {
        anyhow::ensure!(
            !text.contains(needle),
            "probe traffic produced a {needle} series:\n{text}"
        );
    }
    Ok(())
}

/// A client holding stale TLS session state must still probe a node that
/// restarted under the same identity on a fresh port.
///
/// The restarted server cannot decrypt a ticket its predecessor issued, so
/// `rustls` falls back to a full handshake. This is routine on the cache-miss
/// path (`node_origin::probe_candidate`) — an operator restart is not an
/// error — and it was the surviving kernel of the deleted
/// `rejected_0rtt_falls_back_after_server_restart`.
#[tokio::test(flavor = "multi_thread")]
async fn probe_survives_a_server_restart_under_the_same_identity() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let server_sk = SecretKey::generate();
    let (router, server_ep, target, signer, domain, _cache_tmp) =
        spawn_probe_server(&metrics, server_sk.clone()).await?;

    let (client, _client_addr) = local_endpoint(SecretKey::generate()).await?;

    let (first, _rtt) =
        decdn_client_pull::probe::probe_once(&client, target, PROBE_HASH, 0x2001, PROBE_TIMEOUT)
            .await?;
    assert_answer(&first, 0x2001, &signer, &domain)?;

    // The client now holds a ticket for this identity. Tear the server down.
    tokio::time::sleep(TICKET_SETTLE).await;
    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router shutdown: {e}"))?;
    server_ep.close().await;

    // Same node id, fresh port, fresh TLS ticket key: the cached ticket is
    // now undecryptable and must not wedge the probe.
    let (router2, server_ep2, target2, signer2, domain2, _cache_tmp2) =
        spawn_probe_server(&metrics, server_sk).await?;
    let (after, _rtt) =
        decdn_client_pull::probe::probe_once(&client, target2, PROBE_HASH, 0x2002, PROBE_TIMEOUT)
            .await?;
    assert_answer(&after, 0x2002, &signer2, &domain2)?;

    client.close().await;
    router2
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("router shutdown: {e}"))?;
    server_ep2.close().await;

    let text = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    anyhow::ensure!(
        has_metric_line(&text, "decdn_probe_requests_total", 2),
        "both probes must have been served:\n{text}"
    );
    Ok(())
}
