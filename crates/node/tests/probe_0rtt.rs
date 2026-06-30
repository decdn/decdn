//! End-to-end QUIC 0-RTT coverage for `cdn/probe/v1` (ADR 015).
//!
//! Spins a real probe server (iroh `Router` + [`ProbeHandler`] with the
//! 0-RTT master switch on) and drives it with the reusable
//! [`probe_once`] client over a single long-lived client endpoint so the
//! second probe can resume the first probe's TLS session. Asserts the
//! ADR 015 §Observability counters and the approximate session-ticket
//! gauge through the real `OpenMetrics` output.
//!
//! The 0-RTT-*rejected* fallback (ADR 015 §0-RTT Rejection Handling) is
//! covered deterministically by `rejected_0rtt_falls_back_after_server_restart`,
//! using the same-key server-restart technique iroh itself uses in
//! `test_0rtt_after_server_restart`: the client still believes it can
//! resume (same endpoint id ⇒ cached ticket matches) and sends early
//! data, but the restarted server discarded the TLS state needed to
//! decrypt it, so it rejects 0-RTT while still completing the handshake.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use alloy::primitives::{Address, B256, Signature};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, FilesystemOrigin, Hash};
use decdn_client_pull::probe::{ProbeMetrics, probe_once};
use decdn_common::config::ResolvedSecurity;
use decdn_incentive::ProbeSlashData;
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::probe::ProbeHandler;
use decdn_node::handlers::probe_rate_limit::ProbeRateLimiter;
use decdn_node::metrics::Metrics;
use decdn_node::rate_limit::RateLimitConfig;
use decdn_protocol::{ALPN_PROBE, MAX_RATE_PER_MB, SESSION_TICKET_CACHE_SIZE, SLASH_SIG_LEN};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

/// Fixed BLAKE3 digest the empty-cache transport tests query (the
/// blob-backed `warm_0rtt_serves_signed_has_blob_true` queries its real
/// blob hash instead). The transport path (0-RTT vs 1-RTT, RTT, ticket
/// linger) is hash-independent; those servers run an empty cache so every
/// response is `has_blob: false`. Distinct `timestamp_us` values (passed
/// per probe) carry the echoed-back correlation these tests assert on,
/// exactly as the pre-#569 `nonce` did.
const PROBE_HASH: [u8; 32] = [0x5au8; 32];

/// Forwards the client-side 0-RTT transitions into the node metrics
/// registry. Local type + foreign trait → orphan rule satisfied; lets
/// the test assert on the real `decdn_quic_0rtt_*` series.
struct MetricsSink(Arc<Metrics>);

impl ProbeMetrics for MetricsSink {
    fn record_0rtt_attempt(&self) {
        self.0.record_0rtt_attempt();
    }
    fn record_0rtt_accepted(&self) {
        self.0.record_0rtt_accepted();
    }
    fn record_0rtt_rejected(&self) {
        self.0.record_0rtt_rejected();
    }
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

/// Permissive `ProbeRateLimiter` — the 0-RTT suite doesn't exercise the ADR
/// 005 probe rate limiter, so all three layers are effectively unbounded.
fn permissive_probe_rate_limiter(metrics: &Arc<Metrics>) -> Arc<ProbeRateLimiter> {
    let cfg = RateLimitConfig {
        per_peer_rate_per_sec: 1e9,
        per_peer_burst: u32::MAX,
        per_ip_rate_per_sec: 1e9,
        per_ip_burst: u32::MAX,
        global_rate_per_sec: 1e9,
        global_burst: u32::MAX,
        trusted_ips: std::collections::HashSet::new(),
        max_tracked_per_ip: 4096,
        max_tracked_per_peer: 4096,
    };
    Arc::new(ProbeRateLimiter::new(&cfg, Arc::clone(metrics)))
}

/// Server endpoint bound to loopback, relays disabled, with a `Router`
/// serving the probe handler. Takes an explicit `SecretKey` so a test can
/// restart the server under the *same* endpoint identity (the technique
/// iroh's own `test_0rtt_after_server_restart` uses to force a
/// deterministic 0-RTT rejection). Returns the router, the endpoint (so
/// callers can fully `close()` it before a restart), its endpoint id, its
/// dialable address, and the cache directory's `TempDir` guard — callers
/// MUST keep the guard alive for the server's lifetime (dropping it deletes
/// the cache dir out from under the running handler).
async fn spawn_server(
    metrics: &Arc<Metrics>,
    enable_0rtt: bool,
    sk: SecretKey,
) -> anyhow::Result<(
    Router,
    Endpoint,
    iroh::PublicKey,
    SocketAddr,
    tempfile::TempDir,
)> {
    // Empty cache (no origin): every probe answers `has_blob: false`. Most
    // tests here exercise the 0-RTT transport, not content availability — a
    // random EOA signer + deterministic test domain satisfy the post-#569
    // mandatory `slash_sig` (ADR 014 §1) without the suite verifying it.
    // `warm_0rtt_serves_signed_has_blob_true` is the exception and builds
    // its own blob-backed cache + known signer via `spawn_core`.
    let cache_tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(cache_tmp.path(), vec![], 16).await?;
    let eth_signer = Arc::new(PrivateKeySigner::random());
    let (router, ep, id, addr) = spawn_core(
        metrics,
        enable_0rtt,
        sk,
        cache,
        eth_signer,
        test_slash_domain(),
    )
    .await?;
    Ok((router, ep, id, addr, cache_tmp))
}

/// Deterministic test `SlashJudge` EIP-712 domain (Arbitrum Sepolia chain
/// id, fixture verifying-contract address) — mirrors `probe_loopback.rs` so
/// a known signer's `slash_sig` recovers under the same domain.
fn test_slash_domain() -> alloy::dyn_abi::Eip712Domain {
    decdn_incentive::slash_judge_domain(421_614, Address::repeat_byte(0x11))
}

/// Core server bring-up shared by `spawn_server` (empty cache, throwaway
/// signer) and the signed-content test (blob-backed cache, known signer).
/// Returns the router, endpoint, endpoint id, and dialable address; the
/// caller owns the cache's `TempDir` and must keep it alive.
async fn spawn_core(
    metrics: &Arc<Metrics>,
    enable_0rtt: bool,
    sk: SecretKey,
    cache: CacheEngine,
    eth_signer: Arc<PrivateKeySigner>,
    slash_domain: alloy::dyn_abi::Eip712Domain,
) -> anyhow::Result<(Router, Endpoint, iroh::PublicKey, SocketAddr)> {
    let id = sk.public();
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(sk)
        .relay_mode(RelayMode::Disabled)
        // Same ticket-cache size the daemon configures.
        .max_tls_tickets(SESSION_TICKET_CACHE_SIZE)
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

    let handler = Arc::new(ProbeHandler::new(
        id,
        Arc::new(AtomicU64::new(42)),
        Arc::clone(metrics),
        permissive_limiter(metrics),
        permissive_probe_rate_limiter(metrics),
        cache,
        eth_signer,
        slash_domain,
        0,
        MAX_RATE_PER_MB,
        enable_0rtt,
        None, // no stake-lane reservation in the 0-RTT suite (#757)
    ));
    let router = Router::builder(ep.clone())
        .accept(ALPN_PROBE, handler)
        .spawn();
    Ok((router, ep, id, addr))
}

/// Fully tear down a server: stop the router, then close the endpoint so
/// its UDP socket and (crucially for the rejection test) its TLS
/// ticket-decryption state are released before a same-key restart.
async fn shutdown_server(router: Router, ep: Endpoint) -> anyhow::Result<()> {
    router
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    ep.close().await;
    Ok(())
}

/// Long-lived client endpoint with the shared ticket-cache size. Reusing
/// it across probes is what lets a later probe resume an earlier probe's
/// TLS session and go 0-RTT.
async fn client_endpoint() -> anyhow::Result<Endpoint> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .relay_mode(RelayMode::Disabled)
        .max_tls_tickets(SESSION_TICKET_CACHE_SIZE)
        .bind_addr(bind)
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind: {e}"))
}

fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
    let needle = format!("{name} {value}");
    text.lines().any(|l| l == needle)
}

/// Open a cache pre-seeded with `payload` (pulled+verified into the store
/// via a filesystem origin, then the origin dir is dropped so subsequent
/// reads are proven local). Mirrors `probe_loopback.rs::cache_with_blob`.
/// The returned `TempDir` must be kept alive for the cache's lifetime.
async fn cache_with_blob(payload: &[u8]) -> anyhow::Result<(CacheEngine, Hash, tempfile::TempDir)> {
    let hash = Hash::new(payload);
    let origin_dir = tempfile::tempdir()?;
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let dir = origin_dir.path().join(shard);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(hex.as_str()), payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let cache = CacheEngine::open(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;
    let _ = cache.get(hash).await?; // populate the local store
    drop(origin_dir); // prove subsequent reads are local
    Ok((cache, hash, cache_dir))
}

/// Verify a 65-byte `slash_sig` recovers to `signer`'s Ethereum address
/// over the body's frozen signed set (ADR 014 §1). Mirrors
/// `probe_loopback.rs::assert_slash_sig_valid`.
fn assert_slash_sig_valid(
    resp: &decdn_protocol::message::ProbeResponse,
    signer: &PrivateKeySigner,
    domain: &alloy::dyn_abi::Eip712Domain,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        resp.slash_sig.len() == SLASH_SIG_LEN,
        "slash_sig must be exactly {SLASH_SIG_LEN} bytes, got {}",
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

/// Cold probe caches a ticket; the second probe over the same client
/// endpoint resumes it and goes 0-RTT-accepted. Asserts the client
/// counters and the server-side session-ticket gauge.
#[tokio::test(flavor = "multi_thread")]
async fn warm_probe_uses_0rtt_and_records_metrics() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (router, server_ep, server_id, server_addr, _cache_tmp) =
        spawn_server(&metrics, true, SecretKey::generate()).await?;
    let client = client_endpoint().await?;
    let sink = MetricsSink(Arc::clone(&metrics));
    let timeout = Duration::from_secs(5);

    let target = || EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Probe 1 — cold. No cached ticket: 0-RTT not attempted, resolves
    // 1-RTT. The post-exchange linger lets the server's NewSessionTicket
    // reach the client's rustls cache.
    let (resp1, _rtt) = probe_once(
        &client,
        target(),
        PROBE_HASH,
        0xA1,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(resp1.body.timestamp_us, 0xA1);
    assert_eq!(resp1.body.rate_per_mb, 42);

    let text = metrics.encode()?;
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_attempts_total", 0),
        "cold probe must not count a 0-RTT attempt:\n{text}"
    );

    // Probe 2 — warm. Same client endpoint => cached ticket => 0-RTT
    // attempt, accepted by the server.
    let (resp2, _rtt) = probe_once(
        &client,
        target(),
        PROBE_HASH,
        0xB2,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(resp2.body.timestamp_us, 0xB2);

    let text = metrics.encode()?;
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_attempts_total", 1),
        "warm probe must count exactly one 0-RTT attempt:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1),
        "server must have accepted the 0-RTT data:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_rejected_total", 0),
        "no rejection expected on loopback resumption:\n{text}"
    );
    // One distinct client peer completed a 0-RTT-eligible handshake.
    assert!(
        has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 1),
        "session-ticket gauge should reflect one distinct peer:\n{text}"
    );

    client.close().await;
    shutdown_server(router, server_ep).await?;
    Ok(())
}

/// With the master switch off, `probe_once` takes the plain 1-RTT path:
/// the probe still succeeds and no 0-RTT counter moves, even on a warm
/// (ticket-cached) second attempt.
#[tokio::test(flavor = "multi_thread")]
async fn disabled_0rtt_never_attempts_early_data() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Server handler also has 0-RTT off → default full-handshake path.
    let (router, server_ep, server_id, server_addr, _cache_tmp) =
        spawn_server(&metrics, false, SecretKey::generate()).await?;
    let client = client_endpoint().await?;
    let sink = MetricsSink(Arc::clone(&metrics));
    let timeout = Duration::from_secs(5);
    let target = || EndpointAddr::new(server_id).with_ip_addr(server_addr);

    for ts in [0xC3u64, 0xD4u64] {
        let (resp, _rtt) = probe_once(
            &client,
            target(),
            PROBE_HASH,
            ts,
            false,
            Some(&sink),
            timeout,
        )
        .await?;
        assert_eq!(resp.body.timestamp_us, ts);
    }

    let text = metrics.encode()?;
    for name in [
        "decdn_quic_0rtt_attempts_total",
        "decdn_quic_0rtt_accepted_total",
        "decdn_quic_0rtt_rejected_total",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "0-RTT disabled: {name} must stay at zero:\n{text}"
        );
    }
    // Default `on_accepting` path does not touch the gauge.
    assert!(
        has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 0),
        "0-RTT disabled: ticket gauge must stay at zero:\n{text}"
    );

    client.close().await;
    shutdown_server(router, server_ep).await?;
    Ok(())
}

/// ADR 015 §0-RTT Rejection Handling. Force a deterministic server-side
/// 0-RTT rejection with the same technique iroh's own
/// `test_0rtt_after_server_restart` uses: warm a ticket against a server,
/// then restart that server under the *same* `SecretKey` on a fresh
/// endpoint. The client (reusing one endpoint) still has a cached ticket
/// for that endpoint id, so it *attempts* 0-RTT — but the restarted
/// server discarded the TLS ticket-decryption state, so it rejects the
/// early data while still completing the handshake. `probe_once` must
/// classify this as rejected, re-send on the confirmed stream, and still
/// return the correct echoed response.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_0rtt_falls_back_after_server_restart() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Stable identity reused across the restart so the client's cached
    // ticket still matches and it enters the 0-RTT attempt path.
    let server_key = SecretKey::generate();
    let server_id = server_key.public();

    let (router, server_ep, _id, addr1, _cache_tmp1) =
        spawn_server(&metrics, true, server_key.clone()).await?;
    let client = client_endpoint().await?;
    let sink = MetricsSink(Arc::clone(&metrics));
    let timeout = Duration::from_secs(5);

    // Probe 1 — cold (caches a ticket for `server_id`).
    let (r1, _) = probe_once(
        &client,
        EndpointAddr::new(server_id).with_ip_addr(addr1),
        PROBE_HASH,
        0x01,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(r1.body.timestamp_us, 0x01);

    // Probe 2 — warm, accepted (sanity: resumption works pre-restart).
    let (r2, _) = probe_once(
        &client,
        EndpointAddr::new(server_id).with_ip_addr(addr1),
        PROBE_HASH,
        0x02,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(r2.body.timestamp_us, 0x02);
    let text = metrics.encode()?;
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1),
        "pre-restart warm probe should be accepted:\n{text}"
    );

    // Restart the server under the SAME key on a fresh endpoint — new
    // TLS state, so it can no longer decrypt the client's old ticket.
    shutdown_server(router, server_ep).await?;
    let (router2, server_ep2, _id2, addr2, _cache_tmp2) =
        spawn_server(&metrics, true, server_key).await?;

    // Probe 3 — client still has a cached ticket for `server_id`, so it
    // ATTEMPTS 0-RTT (attempts goes 1 → 2), the restarted server REJECTS
    // it, and `probe_once` re-sends on the confirmed stream. The probe
    // still succeeds with the correct echoed timestamp.
    let (r3, _) = probe_once(
        &client,
        EndpointAddr::new(server_id).with_ip_addr(addr2),
        PROBE_HASH,
        0x03,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(
        r3.body.timestamp_us, 0x03,
        "re-sent request must round-trip"
    );

    let text = metrics.encode()?;
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_attempts_total", 2),
        "third probe must have attempted 0-RTT (cached ticket present):\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_rejected_total", 1),
        "restarted same-key server must have rejected the 0-RTT data:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1),
        "accepted count must not move on the rejected probe:\n{text}"
    );

    client.close().await;
    shutdown_server(router2, server_ep2).await?;
    Ok(())
}

/// CHARACTERIZATION TEST — pins iroh 0.98.2's actual 0-RTT behavior so
/// the safety model documented in ADR 015 cannot silently drift from
/// reality (an earlier draft of this PR wrongly claimed per-ALPN 0-RTT
/// gating was "structural via `on_accepting`"; this test exists because
/// that claim was false).
///
/// iroh sets `crypto.max_early_data_size = u32::MAX` on *every* server
/// TLS config (`iroh::tls::make_server_config`), so the QUIC/TLS layer
/// accepts 0-RTT early data for **any** ALPN regardless of whether the
/// handler overrides `on_accepting`. A handler that keeps the default
/// (`accepting.await`, what `enable_0rtt = false` and every non-probe
/// handler use) therefore STILL has the client's 0-RTT *accepted* — it
/// only reads the early-data streams post-handshake instead of pre-.
///
/// Hence 0-RTT replay safety is **client-side only**: the sole code that
/// calls `into_0rtt()` / sends early data is `probe_once`, hard-wired to
/// `ALPN_PROBE` (an idempotent, replay-safe request per ADR 015 §Replay
/// Safety). No `cdn/client/v1` / `cdn/dht/v1` client sends early data, so
/// none can be replayed — server-side `on_accepting` is not the barrier.
///
/// This test asserts the real behavior (`accepted == 1` on a default
/// handler). If a future iroh bump changes it (drops early data on the
/// default path) OR someone makes a non-probe client attempt 0-RTT, the
/// assertions break and force re-evaluation of ADR 015's safety model.
#[tokio::test(flavor = "multi_thread")]
async fn default_on_accepting_still_accepts_0rtt_safety_is_client_side() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Server handler 0-RTT OFF => on_accepting == the default
    // full-handshake path every non-probe ALPN inherits.
    let (router, server_ep, server_id, server_addr, _cache_tmp) =
        spawn_server(&metrics, false, SecretKey::generate()).await?;
    // Client 0-RTT ON => it WILL attempt early data once it has a ticket.
    let client = client_endpoint().await?;
    let sink = MetricsSink(Arc::clone(&metrics));
    let timeout = Duration::from_secs(5);
    let target = || EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Probe 1 — cold: 1-RTT; the default-path server still issues a
    // NewSessionTicket (rustls `send_tls13_tickets > 0`) so the client
    // caches one.
    let (r1, _) = probe_once(
        &client,
        target(),
        PROBE_HASH,
        0xE1,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(r1.body.timestamp_us, 0xE1);

    // Probe 2 — client has a ticket, attempts 0-RTT. Characterization:
    // the default `on_accepting` does NOT prevent acceptance (global
    // max_early_data_size), so the server accepts and the probe rides
    // 0-RTT. Safety here is solely that the request is an idempotent
    // probe — not that the server refused it.
    let (r2, _) = probe_once(
        &client,
        target(),
        PROBE_HASH,
        0xE2,
        true,
        Some(&sink),
        timeout,
    )
    .await?;
    assert_eq!(r2.body.timestamp_us, 0xE2);

    let text = metrics.encode()?;
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_attempts_total", 1),
        "client must have attempted 0-RTT on the warm probe:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1),
        "CHARACTERIZATION: iroh accepts 0-RTT even with the default \
         on_accepting (global max_early_data_size); if this flips, ADR \
         015's client-side-only safety model must be re-derived:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_rejected_total", 0),
        "no rejection: the default path still accepts the 0-RTT:\n{text}"
    );
    // The `enable_0rtt = false` handler returns before
    // `note_session_ticket_peer`, so the gauge stays untouched even
    // though the TLS layer accepted 0-RTT — the switch's only real
    // server-side effect.
    assert!(
        has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 0),
        "disabled handler must not feed the session-ticket gauge:\n{text}"
    );

    client.close().await;
    shutdown_server(router, server_ep).await?;
    Ok(())
}

/// Closes the merge seam #569 ↔ #576 left open: the 0-RTT *early-data*
/// response path (`ZeroRttStatus::Accepted` → `read_response`) is a
/// distinct code path from the 1-RTT `exchange()`, yet no test exercised it
/// carrying a *verified* `slash_sig` and `has_blob: true`. Every other test
/// here uses an empty cache + throwaway signer. Here the server holds the
/// queried blob and signs with a *known* key under the shared test domain,
/// so the warm (0-RTT-accepted) probe asserts the full post-#569 contract —
/// `has_blob: true`, `total_bytes`, and an EIP-712 `slash_sig` that
/// recovers to the operator address — rode the early-data stream intact.
#[tokio::test(flavor = "multi_thread")]
async fn warm_0rtt_serves_signed_has_blob_true() -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let payload = b"deCDN 0-RTT signed-response coverage payload";
    let (cache, blob_hash, _cache_tmp) = cache_with_blob(payload).await?;
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let (router, server_ep, server_id, server_addr) = spawn_core(
        &metrics,
        true,
        SecretKey::generate(),
        cache,
        Arc::clone(&signer),
        domain.clone(),
    )
    .await?;
    let client = client_endpoint().await?;
    let sink = MetricsSink(Arc::clone(&metrics));
    let timeout = Duration::from_secs(5);
    let hash = *blob_hash.as_bytes();
    let target = || EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Probe 1 — cold: 1-RTT, caches a ticket. Already proves the blob is
    // served signed over the plain path.
    let (r1, _) = probe_once(&client, target(), hash, 0xF1, true, Some(&sink), timeout).await?;
    assert_eq!(r1.body.timestamp_us, 0xF1);
    assert!(r1.body.has_blob, "server holds the blob");
    assert_eq!(
        r1.total_bytes,
        Some(payload.len() as u64),
        "size reported when blob present"
    );
    assert_slash_sig_valid(&r1, &signer, &domain)?;

    // Probe 2 — warm: same client endpoint → cached ticket → 0-RTT
    // attempt, accepted. The signed content-availability body must survive
    // the early-data path byte-for-byte.
    let (r2, _) = probe_once(&client, target(), hash, 0xF2, true, Some(&sink), timeout).await?;
    assert_eq!(r2.body.timestamp_us, 0xF2);
    assert!(r2.body.has_blob, "0-RTT path must still report has_blob");
    assert_eq!(r2.total_bytes, Some(payload.len() as u64));
    assert_slash_sig_valid(&r2, &signer, &domain)?;

    let text = metrics.encode()?;
    assert!(
        has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1),
        "second probe must have been served as accepted 0-RTT:\n{text}"
    );

    client.close().await;
    shutdown_server(router, server_ep).await?;
    Ok(())
}
