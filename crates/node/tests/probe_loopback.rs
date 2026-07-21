//! Two-endpoint loopback test for `cdn/probe/v1`.
//!
//! Spawns a server endpoint running the probe handler, connects a client
//! endpoint over iroh on localhost, sends a `ProbeRequest`, and verifies the
//! response echoes the request, reports content availability, and carries a
//! valid EIP-712 `slash_sig` (ADR 005 / ADR 014, #318).

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, FilesystemOrigin, Hash};
use decdn_common::config::ResolvedSecurity;
use decdn_incentive::ProbeSlashData;
use decdn_node::dht::routing::NodeId;
use decdn_node::dht::staker_set::{ConfigStakerSet, StakerSet};
use decdn_node::dispatch::{ConnectionLimiter, RejectReason};
use decdn_node::handlers::probe::{ProbeHandler, StakeLanePolicy};
use decdn_node::handlers::probe_rate_limit::{ProbeRateLimiter, ProbeRejectLayer};
use decdn_node::metrics::Metrics;
use decdn_node::rate_limit::RateLimitConfig;
use decdn_protocol::{
    ALPN_PROBE, APP_ERR_RATE_LIMITED, MAX_MESSAGE_SIZE, MAX_RATE_PER_MB, ProbeMessage,
    SLASH_SIG_LEN, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse, ProbeResponseBody},
    read_frame, write_frame,
};
use iroh::endpoint::{
    ApplicationClose, Connection, ConnectionError, IdleTimeout, QuicTransportConfig, ReadError,
    ReadToEndError, VarInt, presets,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use tokio::task::JoinHandle;

/// Deterministic test `SlashJudge` EIP-712 domain (Arbitrum Sepolia chain id,
/// fixture verifying-contract address).
fn test_slash_domain() -> Eip712Domain {
    decdn_incentive::slash_judge_domain(421_614, Address::repeat_byte(0x11))
}

/// Open an empty cache (no origin) in a fresh temp dir. The returned
/// `TempDir` must be kept alive for the cache's lifetime.
async fn empty_cache() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), vec![], 16).await?;
    Ok((cache, tmp))
}

/// Open a cache pre-seeded with `payload` (pulled+verified into the store via
/// a filesystem origin, then the origin dir is dropped). Returns the cache,
/// the blob hash, and the temp dirs to keep alive.
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

/// Open a cache pre-seeded with two distinct blobs from a single filesystem
/// origin (one engine per dir — iroh-blobs is single-writer). Returns the
/// cache, both hashes, and the cache temp dir to keep alive.
async fn cache_with_two_blobs(
    a: &[u8],
    b: &[u8],
) -> anyhow::Result<(CacheEngine, Hash, Hash, tempfile::TempDir)> {
    let origin_dir = tempfile::tempdir()?;
    for payload in [a, b] {
        let hex = Hash::new(payload).to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let dir = origin_dir.path().join(shard);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(hex.as_str()), payload)?;
    }

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let cache = CacheEngine::open(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;
    let (ha, hb) = (Hash::new(a), Hash::new(b));
    let _ = cache.get(ha).await?; // populate the local store
    let _ = cache.get(hb).await?;
    drop(origin_dir);
    Ok((cache, ha, hb, cache_dir))
}

/// Parse the integer value of an `OpenMetrics` counter/gauge line
/// (`<name> <value>`) out of the encoded exposition text. Returns `None` if
/// the metric is absent — distinct from `Some(0)` so a missing counter is
/// never mistaken for an un-incremented one.
fn metric_value(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?.strip_prefix(' ')?;
        rest.trim().parse().ok()
    })
}

/// Build a `ProbeHandler` with the given delivery bounds, returning the
/// handler plus the random signer and domain so tests can verify `slash_sig`.
#[allow(clippy::too_many_arguments)]
fn build_handler_bounds(
    server_id: iroh::PublicKey,
    rate: u64,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    floor: u64,
    ceiling: u64,
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    // Most tests don't exercise the ADR 005 probe rate limiter — wire a
    // permissive one so only the layer under test (the `ConnectionLimiter`,
    // hold budget, rate clamp, etc.) can fire.
    build_handler_with_probe_limiter(
        server_id,
        rate,
        metrics,
        limiter,
        permissive_probe_rate_limiter(metrics),
        cache,
        floor,
        ceiling,
    )
}

/// Like [`build_handler_bounds`] but with an explicit probe rate limiter, so a
/// test can install a strict [`ProbeRateLimiter`] (ADR 005 §Probe rate
/// limiting).
#[allow(clippy::too_many_arguments)]
fn build_handler_with_probe_limiter(
    server_id: iroh::PublicKey,
    rate: u64,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    probe_limiter: Arc<ProbeRateLimiter>,
    cache: CacheEngine,
    floor: u64,
    ceiling: u64,
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        Arc::new(AtomicU64::new(rate)),
        Arc::clone(metrics),
        limiter,
        probe_limiter,
        cache,
        Arc::clone(&signer),
        domain.clone(),
        decdn_node::rate_bounds::RateBounds::new(floor, ceiling),
        // This suite is the pre-ADR-015 1-RTT loopback coverage; 0-RTT
        // acceptance has its own dedicated test (probe_0rtt.rs).
        false,
        // No stake-lane reservation for the general-purpose builder; the
        // dedicated reservation tests use `build_handler_with_lane` (#757).
        None,
    ));
    (handler, signer, domain)
}

/// Build a `ProbeHandler` carrying a stake-lane reservation policy (#757).
/// `stakers` seeds an in-memory [`ConfigStakerSet`]; a probe whose client
/// `NodeId` is in that set is treated as a stake-lane (node-to-node)
/// requester and never reserved out.
#[allow(clippy::too_many_arguments)]
fn build_handler_with_lane(
    server_id: iroh::PublicKey,
    rate: u64,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    stakers: std::collections::HashSet<NodeId>,
    reserved_holds: NonZeroUsize,
    max_holds: usize,
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(stakers));
    let policy = StakeLanePolicy::new(staker_set, reserved_holds, max_holds);
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        Arc::new(AtomicU64::new(rate)),
        Arc::clone(metrics),
        limiter,
        permissive_probe_rate_limiter(metrics),
        cache,
        Arc::clone(&signer),
        domain.clone(),
        decdn_node::rate_bounds::RateBounds::new(0, MAX_RATE_PER_MB),
        false,
        Some(policy),
    ));
    (handler, signer, domain)
}

/// Default-bounds handler (no effective rate clamp).
fn build_handler(
    server_id: iroh::PublicKey,
    rate: u64,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    build_handler_bounds(server_id, rate, metrics, limiter, cache, 0, MAX_RATE_PER_MB)
}

/// Build a permissive `ConnectionLimiter` suitable for tests that don't
/// exercise rate-limiting behaviour.
fn permissive_limiter(metrics: &Arc<Metrics>) -> Arc<ConnectionLimiter> {
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1_000_000.0,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(metrics)))
}

/// Build a permissive `ProbeRateLimiter` for tests that don't exercise the
/// ADR 005 probe rate-limiting behaviour (all layers effectively unbounded).
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

fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

/// Build an endpoint bound to 127.0.0.1 with relays disabled and no discovery.
/// Returns the endpoint plus its local socket address.
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

/// Verify a 65-byte `slash_sig` recovers to `signer`'s Ethereum address over
/// the body's frozen signed set (ADR 014 §1).
fn assert_slash_sig_valid(
    resp: &ProbeResponse,
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
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

#[tokio::test(flavor = "multi_thread")]
async fn probe_roundtrip() -> anyhow::Result<()> {
    let rate_per_mb: u64 = 42;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (cache, _cache_tmp) = empty_cache().await?;
    let (handler, signer, domain) = build_handler(server_id, rate_per_mb, &metrics, limiter, cache);

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;

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

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = ProbeRequest {
        hash: [0x5au8; 32],
        timestamp_us: 0x00c0_ffee,
    };
    let payload = encode_message(&ProbeMessage::Request(req))?;
    write_frame(&mut send, &payload)
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
    assert_eq!(resp.body.rate_per_mb, rate_per_mb, "rate (unclamped)");
    assert!(
        !resp.body.has_blob,
        "empty cache must report has_blob=false"
    );
    assert_eq!(resp.total_bytes, None, "no size when blob absent");
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    conn.close(0u32.into(), b"bye");
    client_ep.close().await;

    // Allow the server task to finish handling before closing its endpoint.
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

/// Spin up a probe server and return the client's connected [`Connection`]
/// plus the background accept task and endpoints. The accept task is expected
/// to return `Err` once the server handler rejects the client's input — that
/// is the signal the correct app error code was emitted.
struct Harness {
    client_conn: Connection,
    accept_task: JoinHandle<anyhow::Result<()>>,
    client_ep: Endpoint,
    server_ep: Endpoint,
    /// Kept alive so the cache dir outlives the connection.
    _cache_tmp: tempfile::TempDir,
}

async fn spin_up_probe_harness() -> anyhow::Result<Harness> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (cache, cache_tmp) = empty_cache().await?;
    let (handler, _signer, _domain) = build_handler(server_id, 1, &metrics, limiter, cache);
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;

    let server_ep_bg = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        let incoming = server_ep_bg
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("no incoming connection"))?;
        let connecting = incoming
            .accept()
            .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
        let conn = connecting
            .await
            .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
        handler
            .accept(conn)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    });

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let client_conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    Ok(Harness {
        client_conn,
        accept_task,
        client_ep,
        server_ep,
        _cache_tmp: cache_tmp,
    })
}

/// Expect `recv.read_to_end` to fail with `expected_code` delivered either as a
/// stream `RESET_STREAM` or as a connection-level `CONNECTION_CLOSE` carrying
/// an application error code. The probe handler closes the connection with
/// the same app code it resets the stream with (probe is 1:1
/// connection:stream), so either form is a correct observation of the ADR 013
/// mapping.
async fn assert_reset_with_code(
    recv: &mut iroh::endpoint::RecvStream,
    expected_code: u32,
) -> anyhow::Result<()> {
    let expected = VarInt::from_u32(expected_code);
    match recv.read_to_end(4096).await {
        Err(ReadToEndError::Read(ReadError::Reset(code))) if code == expected => Ok(()),
        Err(ReadToEndError::Read(ReadError::ConnectionLost(
            ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. }),
        ))) if error_code == expected => Ok(()),
        other => {
            anyhow::bail!("expected error carrying app code {expected_code:#x}, got {other:?}")
        }
    }
}

async fn tear_down(h: Harness) -> anyhow::Result<()> {
    h.client_conn.close(0u32.into(), b"bye");
    h.client_ep.close().await;
    // Handler is expected to return Err on these error-path tests; we only
    // need to confirm the task joined, not that it succeeded.
    let _ = h.accept_task.await;
    h.server_ep.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_oversized_frame_returns_too_large_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Write a varint length-prefix that exceeds MAX_MESSAGE_SIZE with no payload.
    let mut bogus = Vec::new();
    let mut v = MAX_MESSAGE_SIZE + 1;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            bogus.push(byte);
            break;
        }
        bogus.push(byte | 0x80);
    }
    send.write_all(&bogus)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x02).await?;
    tear_down(h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_garbage_postcard_returns_malformed_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Discriminant 0x00 is `Request`, a known variant, but its body
    // (`hash` 32 bytes + `timestamp_us`) is truncated to a single byte —
    // postcard hits end-of-input mid-struct. A genuine parse fault → 0x03,
    // distinct from an unknown discriminant (see
    // `probe_unknown_discriminant_returns_unsupported_code`).
    write_frame(&mut send, &[0x00u8, 0x01])
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x03).await?;
    tear_down(h).await
}

/// ADR 013 §Application Error Codes: an unknown enum discriminant is the
/// Tier-2 graceful-evolution signal — `UNSUPPORTED_MESSAGE` (0x01), NOT
/// `MALFORMED_MESSAGE` (0x03). Pairs
/// `probe_garbage_postcard_returns_malformed_code` (genuine parse fault under
/// a known discriminant).
#[tokio::test(flavor = "multi_thread")]
async fn probe_unknown_discriminant_returns_unsupported_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Discriminant 99 is far past ProbeMessage's two declared variants.
    write_frame(&mut send, &[99u8, 0])
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x01).await?;
    tear_down(h).await
}

/// #577 M1 — a transport-level truncation (client promises `len` bytes
/// then finishes the stream early) hits `FrameError::Io(UnexpectedEof)`
/// in `read_frame`, which must surface as `APP_ERR_NO_ERROR` (0x00),
/// not `APP_ERR_MALFORMED_MESSAGE` (0x03). A dropped connection is not
/// a protocol fault; collapsing the two would push peers toward the
/// wrong backoff/penalty discipline. Pairs the existing
/// `probe_garbage_postcard_returns_malformed_code` (genuine
/// `Decode`-class fault, 0x03) and
/// `probe_read_timeout_resets_stream_with_zero_code` (timeout, 0x00).
#[tokio::test(flavor = "multi_thread")]
async fn probe_io_truncated_frame_returns_zero_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Write a varint length prefix promising 100 bytes of payload, then
    // write only 3 bytes and finish the stream. The server's
    // `read_frame` reads the varint, allocates the buffer, then
    // `read_exact` short-reads → `FrameError::Io(UnexpectedEof)`.
    let mut bogus = Vec::new();
    let mut v: u32 = 100;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            bogus.push(byte);
            break;
        }
        bogus.push(byte | 0x80);
    }
    bogus.extend_from_slice(b"abc");
    send.write_all(&bogus)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x00).await?;
    tear_down(h).await
}

#[tokio::test(flavor = "multi_thread")]
async fn probe_response_on_server_stream_returns_unsupported_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Well-formed frame, but the wrong variant (server expects Request).
    let payload = encode_message(&ProbeMessage::Response(ProbeResponse {
        body: ProbeResponseBody {
            hash: [0u8; 32],
            has_blob: false,
            rate_per_mb: 0,
            timestamp_us: 0,
        },
        total_bytes: None,
        slash_sig: vec![0u8; SLASH_SIG_LEN],
    }))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;

    assert_reset_with_code(&mut recv, 0x01).await?;
    tear_down(h).await
}

// Closes #241. ProbeHandler has two phase-level timeouts that had no test
// coverage: ACCEPT_BI_TIMEOUT (client connected but never opened a
// bi-stream) and PROBE_READ_TIMEOUT (client opened a stream but never
// wrote a frame). Both are 5s at runtime; gating tests on the real
// deadline would slow every CI run, so instead each test uses
// `tokio::time::pause()` + `advance()` to fast-forward the handler's
// inner `tokio::time::timeout` future by 6s of virtual time. iroh's
// network I/O sits on tokio-mio (not tokio::time), so only the timeout
// futures we care about are affected.

#[tokio::test(start_paused = true)]
async fn probe_read_timeout_resets_stream_with_zero_code() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    let (mut send, mut recv) = h
        .client_conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    // Deliberately don't write a frame. Virtual-advance past the handler's
    // PROBE_READ_TIMEOUT so the timeout fires without the test blocking
    // on the real 5-second deadline. `start_paused = true` requires the
    // current_thread runtime; iroh doesn't insist on multi-thread.
    tokio::time::advance(Duration::from_secs(6)).await;

    // Handler resets the stream AND closes the connection with app code 0
    // (ADR 013 defines no timeout-specific code; the handler uses 0 for
    // "no app error"). `assert_reset_with_code` tolerates either
    // observation form.
    assert_reset_with_code(&mut recv, 0x00).await?;
    let _ = send.finish();
    tear_down(h).await
}

#[tokio::test(start_paused = true)]
async fn probe_accept_bi_timeout_errors_handler() -> anyhow::Result<()> {
    let h = spin_up_probe_harness().await?;
    // Deliberately do NOT call open_bi. The handler's first await is
    // `tokio::time::timeout(ACCEPT_BI_TIMEOUT, conn.accept_bi())`, which
    // must time out and return Err.
    tokio::time::advance(Duration::from_secs(6)).await;

    // Confirm the server task returned Err with the expected message.
    // Just `.await` — no wrapper timeout, because under `start_paused`
    // `tokio::time::timeout` itself runs on the virtual clock and would
    // not trip on a non-timer deadlock. Cargo's test-harness global
    // timeout covers that pathological case.
    let joined = h
        .accept_task
        .await
        .map_err(|e| anyhow::anyhow!("join: {e}"))?;
    let Err(err) = joined else {
        anyhow::bail!("handler should have returned Err on ACCEPT_BI_TIMEOUT, got Ok");
    };
    let msg = err.to_string();
    anyhow::ensure!(
        msg.contains("accept_bi timed out"),
        "expected accept_bi timeout error, got: {msg}"
    );

    // Manual teardown — `h.accept_task` was consumed above, so the shared
    // `tear_down` helper can't run as-is.
    h.client_conn.close(0u32.into(), b"bye");
    h.client_ep.close().await;
    h.server_ep.close().await;
    Ok(())
}

/// End-to-end check that the per-source rate limiter rejects with
/// `APP_ERR_RATE_LIMITED` (`0x10`) on the wire. Without this, the dispatch
/// reject path is dead code under tests — every other test in this file uses
/// `permissive_limiter`.
///
/// Strict per-IP burst=1 limiter. The bucket for the loopback source key is
/// drained out-of-band via the limiter's test hook, so the single live client
/// connection is unconditionally rejected by `ConnectionLimiter::acquire` at
/// the top of `serve` and observes `APP_ERR_RATE_LIMITED` on its
/// `CONNECTION_CLOSE`.
///
/// Draining the bucket directly — rather than via a throwaway first connection
/// that races a second one — keeps this deterministic: the earlier
/// two-connection form flaked under `nextest` because the second connection's
/// establishment raced the first's teardown on the shared client endpoint, and
/// the accept loop assumed it would receive exactly two `Incoming`s.
#[tokio::test(flavor = "multi_thread")]
async fn probe_rate_limit_returns_rate_limited_close_code() -> anyhow::Result<()> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());

    // burst=1 per-source so the loopback source key rejects after a single
    // charge. Global is loose so it doesn't interfere.
    let strict = ResolvedSecurity {
        max_concurrent_handlers: 64,
        per_source_rate_per_sec: 0.001, // negligible refill within the test window
        per_source_burst: 1,
        max_tracked_sources: 32,
    };
    let limiter = Arc::new(ConnectionLimiter::new(&strict, Arc::clone(&metrics)));

    // Pre-drain the per-source bucket for the loopback source key. The client
    // endpoint binds to 127.0.0.1, so the server resolves the connection's
    // source key to `127.0.0.1` (`source_key` leaves IPv4 unchanged); charging
    // it here exhausts the burst-1 budget before the live connection arrives.
    // Dropping the returned permit releases only the global semaphore slot —
    // the consumed per-source token is time-based and stays spent for the full
    // refill period (1 / `per_source_rate_per_sec`, ~1000s here).
    drop(
        limiter
            .acquire_for_test(Some(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST)))
            .map_err(|r| anyhow::anyhow!("pre-drain unexpectedly rejected: {r:?}"))?,
    );

    let limiter_probe = Arc::clone(&limiter);
    let (cache, _cache_tmp) = empty_cache().await?;
    let (handler, _signer, _domain) = build_handler(server_id, 1, &metrics, limiter, cache);

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;
    let server_ep_bg = server_ep.clone();
    let handler_bg = Arc::clone(&handler);
    let accept_task = tokio::spawn(async move {
        let incoming = server_ep_bg
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("no incoming connection"))?;
        let connecting = incoming
            .accept()
            .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
        let conn = connecting
            .await
            .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
        handler_bg
            .accept(conn)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok::<_, anyhow::Error>(())
    });

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // The only connection: the limiter rejects it on accept and the server
    // closes with APP_ERR_RATE_LIMITED.
    let conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    // `closed()` resolves with the application close code. Assert on the
    // close reason bytes too, not just the 0x10 code: the reject path stamps
    // `RejectReason::as_str()` into the frame, so checking it proves the
    // *per-source* layer fired rather than the (also-0x10) global cap.
    let close_err = conn.closed().await;
    let expected = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    match close_err {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, reason })
            if error_code == expected
                && reason.as_ref() == RejectReason::PerSource.as_str().as_bytes() => {}
        other => {
            anyhow::bail!("expected ApplicationClosed({expected:?}, \"per-source\"), got {other:?}")
        }
    }
    // The live connection must have charged the *same* pre-drained key, not a
    // fresh one: a single tracked source confirms `peer_ip` resolved it to
    // 127.0.0.1 (a different key would have been admitted, not rejected).
    assert_eq!(
        limiter_probe.per_source_tracked(),
        1,
        "live connection must hit the pre-drained 127.0.0.1 bucket"
    );

    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

/// End-to-end check that the ADR 005 §Probe rate limiting three-layer limiter
/// (#982) — distinct from the `ConnectionLimiter` exercised above — rejects a
/// probe with `APP_ERR_RATE_LIMITED` (`0x10`) and the `per_peer` layer label on
/// the wire.
///
/// Strict per-peer burst=1 probe limiter; the per-peer bucket for the client's
/// `NodeId` is pre-drained out-of-band via `ProbeRateLimiter::check` (the
/// per-peer layer is keyed by `NodeId` independent of IP, so this is robust to
/// loopback path selection). The single live connection from that `NodeId` is
/// then unconditionally rejected at the per-peer layer. Draining directly —
/// rather than via a throwaway first connection — keeps this deterministic, the
/// same rationale as `probe_rate_limit_returns_rate_limited_close_code`.
#[tokio::test(flavor = "multi_thread")]
async fn probe_three_layer_limiter_rejects_per_peer() -> anyhow::Result<()> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());

    // Permissive connection limiter so only the probe three-layer limiter can
    // fire; strict per-peer (burst=1, negligible refill) probe limiter.
    let conn_limiter = permissive_limiter(&metrics);
    let strict_probe = RateLimitConfig {
        per_peer_rate_per_sec: 0.001, // negligible refill within the test window
        per_peer_burst: 1,
        per_ip_rate_per_sec: 1e9,
        per_ip_burst: u32::MAX,
        global_rate_per_sec: 1e9,
        global_burst: u32::MAX,
        trusted_ips: std::collections::HashSet::new(),
        max_tracked_per_ip: 4096,
        max_tracked_per_peer: 4096,
    };
    let probe_limiter = Arc::new(ProbeRateLimiter::new(&strict_probe, Arc::clone(&metrics)));

    // Pin the client key so we know its NodeId before connecting, then drain
    // the per-peer bucket for it. `peer_ip = None` skips the (loose) per-IP
    // layer; the per-peer charge is all we need.
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let client_node = NodeId::from_bytes(*client_id.as_bytes());
    probe_limiter
        .check(&client_node, None)
        .map_err(|l| anyhow::anyhow!("pre-drain unexpectedly rejected: {l:?}"))?;

    let (cache, _cache_tmp) = empty_cache().await?;
    let (handler, _signer, _domain) = build_handler_with_probe_limiter(
        server_id,
        1,
        &metrics,
        conn_limiter,
        Arc::clone(&probe_limiter),
        cache,
        0,
        MAX_RATE_PER_MB,
    );

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;
    let server_ep_bg = server_ep.clone();
    let handler_bg = Arc::clone(&handler);
    let accept_task = tokio::spawn(async move {
        let incoming = server_ep_bg
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("no incoming connection"))?;
        let connecting = incoming
            .accept()
            .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
        let conn = connecting
            .await
            .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
        handler_bg
            .accept(conn)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok::<_, anyhow::Error>(())
    });

    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    // Assert both the 0x10 code and the `per_peer` reason bytes: the probe
    // reject path stamps `RejectLayer::as_str()` into the close frame, so the
    // label proves the *per-peer* layer fired (the gap #982 closed) rather than
    // the also-0x10 per-IP or global cap.
    let close_err = conn.closed().await;
    let expected = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    match close_err {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, reason })
            if error_code == expected
                && reason.as_ref() == ProbeRejectLayer::PerPeer.as_str().as_bytes() => {}
        other => {
            anyhow::bail!("expected ApplicationClosed({expected:?}, \"per_peer\"), got {other:?}")
        }
    }
    let scrape = metrics.encode().map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(
        metric_value(&scrape, "decdn_probe_rate_limit_rejected_per_peer_total"),
        Some(1),
        "probe per-peer rejection must appear in /metrics scrape:\n{scrape}"
    );

    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}

/// Drive one full probe exchange against `handler` on a loopback pair and
/// return the decoded response. Handles endpoint setup/teardown.
async fn run_one_probe(
    server_sk: SecretKey,
    handler: Arc<ProbeHandler>,
    req: ProbeRequest,
) -> anyhow::Result<ProbeResponse> {
    run_one_probe_as(fresh_key(), server_sk, handler, req).await
}

/// Like [`run_one_probe`] but with an explicit client `SecretKey`, so a test
/// can place the client's `NodeId` into the handler's staker set and exercise
/// the stake-lane probe-acceptance path (#757).
async fn run_one_probe_as(
    client_sk: SecretKey,
    server_sk: SecretKey,
    handler: Arc<ProbeHandler>,
    req: ProbeRequest,
) -> anyhow::Result<ProbeResponse> {
    let server_id = server_sk.public();
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;
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

    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    write_frame(&mut send, &encode_message(&ProbeMessage::Request(req))?)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    send.finish().map_err(|e| anyhow::anyhow!("finish: {e}"))?;
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read: {e}"))?;
    let (msg, _rest) = decode_message::<ProbeMessage>(&frame)?;
    let resp = match msg {
        ProbeMessage::Response(r) => r,
        ProbeMessage::Request(_) => anyhow::bail!("unexpected request variant on client"),
    };
    conn.close(0u32.into(), b"bye");
    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(resp)
}

/// A node holding the blob signs `has_blob: true`, reports `total_bytes`,
/// and the `slash_sig` verifies (ADR 005 §`cdn/probe/v1`, #318).
#[tokio::test(flavor = "multi_thread")]
async fn probe_has_blob_true_for_cached_blob() -> anyhow::Result<()> {
    let payload = b"probe-served content-addressed bytes";
    let (cache, hash, _cache_tmp) = cache_with_blob(payload).await?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (handler, signer, domain) = build_handler(server_id, 7, &metrics, limiter, cache);

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0xabc_def,
    };
    let resp = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(resp.body.has_blob, "cached blob must report has_blob=true");
    anyhow::ensure!(
        resp.total_bytes == Some(payload.len() as u64),
        "total_bytes should report the blob size, got {:?}",
        resp.total_bytes
    );
    anyhow::ensure!(resp.body.hash == *hash.as_bytes(), "hash echoed");
    assert_slash_sig_valid(&resp, &signer, &domain)?;
    Ok(())
}

/// A node with the eviction-hold path **disabled by config**
/// (`max_probe_holds == 0`) holds the blob but cannot guarantee a hold, so it
/// must still answer `has_blob: false` with a valid `slash_sig` over
/// `has_blob=false` — never risk a phantom slash (ADR 005 §Hold budget).
/// Exercises the handler's `HoldsDisabled` arm end-to-end and asserts the
/// outcome is counted as a *disabled* event, NOT as budget pressure (#739).
#[tokio::test(flavor = "multi_thread")]
async fn probe_holds_disabled_signs_has_blob_false_and_counts_disabled() -> anyhow::Result<()> {
    let payload = b"present but un-holdable";
    let (cache, hash, _cache_tmp) = cache_with_blob(payload).await?;
    cache.set_max_probe_holds(0); // disable holds -> HoldsDisabled

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (handler, signer, domain) = build_handler(server_id, 7, &metrics, limiter, cache);

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0x1234,
    };
    let resp = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        !resp.body.has_blob,
        "holds-disabled must yield has_blob=false even though the blob is cached"
    );
    anyhow::ensure!(
        resp.total_bytes.is_none(),
        "no size advertised when has_blob=false, got {:?}",
        resp.total_bytes
    );
    // The signature must cover has_blob=false (not a stale true).
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    // An intentional disable must increment the disabled counter and leave
    // the budget-pressure counter at zero — otherwise the "increase
    // max_probe_holds" alert fires on a config the operator chose.
    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_holds_disabled_total") == Some(1),
        "holds-disabled probe must bump decdn_probe_holds_disabled_total:\n{text}"
    );
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_hold_violations_total") == Some(0),
        "an intentional disable must NOT inflate the budget-pressure counter:\n{text}"
    );
    Ok(())
}

/// A node with a positive but fully-occupied hold budget answers
/// `has_blob: false` and counts the event as genuine budget pressure
/// (`probe_hold_violations`), NOT as a config disable (#739). This is the
/// signal whose alert remedy is "increase `max_probe_holds`".
#[tokio::test(flavor = "multi_thread")]
async fn probe_budget_exhausted_counts_violation_not_disabled() -> anyhow::Result<()> {
    let a: &[u8] = b"first popular blob";
    let b: &[u8] = b"second popular blob";
    let (cache, ha, hb, _cache_tmp) = cache_with_two_blobs(a, b).await?;
    // One slot, two distinct cached blobs: the first hold fills the budget,
    // the second probe finds it exhausted (max > 0).
    cache.set_max_probe_holds(1);
    anyhow::ensure!(
        cache.try_probe_hold(ha).await? == decdn_cache::ProbeHoldOutcome::Held,
        "first hold should fit the budget"
    );

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (handler, signer, domain) = build_handler(server_id, 7, &metrics, limiter, cache);

    let req = ProbeRequest {
        hash: *hb.as_bytes(),
        timestamp_us: 0x55,
    };
    let resp = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        !resp.body.has_blob,
        "budget-exhausted hold must yield has_blob=false even though the blob is cached"
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_hold_violations_total") == Some(1),
        "genuine budget exhaustion must bump decdn_probe_hold_violations_total:\n{text}"
    );
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_holds_disabled_total") == Some(0),
        "budget pressure must NOT be counted as a config disable:\n{text}"
    );
    Ok(())
}

/// With a stake-lane reservation configured (#757, ADR 003 §Admission and
/// Priority), a probe from a client that is NOT a registered operator is
/// answered `has_blob: false` once hold usage reaches the end-client ceiling.
/// Here `max_holds=1, reserved=1` gives a ceiling of `0`, so the end-client
/// is shed immediately even though the blob is cached and the budget is free.
/// The event is counted as a stake-lane reservation — never as budget
/// exhaustion (`probe_hold_violations`) or a config disable
/// (`probe_holds_disabled`), whose alerts have different remedies.
#[tokio::test(flavor = "multi_thread")]
async fn probe_end_client_reserved_out_signs_has_blob_false() -> anyhow::Result<()> {
    let payload = b"reserved-for-stake-lane content";
    let (cache, hash, _cache_tmp) = cache_with_blob(payload).await?;
    cache.set_max_probe_holds(1);

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    // Empty staker set => the fresh client key is an unregistered end-client.
    // reserved=1, max=1 => end-client ceiling is 0, so any end-client probe
    // under any hold usage (including zero) is reserved out.
    let (handler, signer, domain) = build_handler_with_lane(
        server_id,
        7,
        &metrics,
        limiter,
        cache,
        std::collections::HashSet::new(),
        NonZeroUsize::MIN,
        1,
    );

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0x9001,
    };
    let resp = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        !resp.body.has_blob,
        "an end-client under a stake-lane reservation must get has_blob=false"
    );
    anyhow::ensure!(
        resp.total_bytes.is_none(),
        "no size advertised when has_blob=false, got {:?}",
        resp.total_bytes
    );
    // The signature must cover has_blob=false (never a stale true).
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_stake_lane_reserved_total") == Some(1),
        "reservation refusal must bump decdn_probe_stake_lane_reserved_total:\n{text}"
    );
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_hold_violations_total") == Some(0),
        "a stake-lane reservation must NOT be counted as budget pressure:\n{text}"
    );
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_holds_disabled_total") == Some(0),
        "a stake-lane reservation must NOT be counted as a config disable:\n{text}"
    );
    Ok(())
}

/// Under the very same reservation that sheds an end-client, a probe from a
/// registered operator (its `NodeId` in the staker set) is admitted: it
/// passes the reservation gate, takes a hold, and signs `has_blob: true`
/// (#757). This is the headroom the reservation exists to protect for
/// node-to-node cache-miss probes.
#[tokio::test(flavor = "multi_thread")]
async fn probe_stake_lane_requester_keeps_reserved_headroom() -> anyhow::Result<()> {
    let payload = b"served to a node-to-node requester";
    let (cache, hash, _cache_tmp) = cache_with_blob(payload).await?;
    cache.set_max_probe_holds(1);

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let client_sk = fresh_key();
    let client_node_id = NodeId::from_bytes(*client_sk.public().as_bytes());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    // Same aggressive reservation (reserved=1, max=1) as the end-client test,
    // but this requester IS a registered operator.
    let mut stakers = std::collections::HashSet::new();
    stakers.insert(client_node_id);
    let (handler, signer, domain) = build_handler_with_lane(
        server_id,
        7,
        &metrics,
        limiter,
        cache,
        stakers,
        NonZeroUsize::MIN,
        1,
    );

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0x9002,
    };
    let resp = run_one_probe_as(client_sk, server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.has_blob,
        "a stake-lane requester must keep its reserved headroom -> has_blob=true"
    );
    anyhow::ensure!(
        resp.total_bytes == Some(payload.len() as u64),
        "stake-lane requester should be served the blob size, got {:?}",
        resp.total_bytes
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(&text, "decdn_probe_stake_lane_reserved_total").unwrap_or(0) == 0,
        "a stake-lane requester must NOT trip the reservation counter:\n{text}"
    );
    Ok(())
}

/// `rate_per_mb` is clamped to the configured delivery ceiling before
/// signing, and the `slash_sig` covers the clamped value (ADR 005 §Rate
/// bounds validation, #318).
#[tokio::test(flavor = "multi_thread")]
async fn probe_rate_clamped_to_ceiling_before_signing() -> anyhow::Result<()> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (cache, _cache_tmp) = empty_cache().await?;
    // Configured rate 42 but a ceiling of 5 → response must quote 5.
    let (handler, signer, domain) =
        build_handler_bounds(server_id, 42, &metrics, limiter, cache, 0, 5);

    let req = ProbeRequest {
        hash: [9u8; 32],
        timestamp_us: 99,
    };
    let resp = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.rate_per_mb == 5,
        "rate must be clamped to ceiling 5, got {}",
        resp.body.rate_per_mb
    );
    // slash_sig must verify over the clamped rate, not the raw 42.
    assert_slash_sig_valid(&resp, &signer, &domain)?;
    Ok(())
}

/// Verify that `QuicTransportConfig::max_idle_timeout` actually closes a
/// silent connection (the wiring `quic_transport_config` relies on).
/// The runtime value is 30s per ADR 005; we shorten it to 300ms here so
/// the test runs in well under a second. `keep_alive_interval` is parked
/// at 60s on both ends so the path stays silent across the idle window —
/// otherwise the keep-alive PINGs the runtime sends would refresh the
/// timer and the test could never observe the close.
#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_closes_quiet_connection() -> anyhow::Result<()> {
    let idle = Duration::from_millis(300);
    let build_cfg = || -> anyhow::Result<QuicTransportConfig> {
        let it: IdleTimeout = idle
            .try_into()
            .map_err(|e| anyhow::anyhow!("idle timeout: {e}"))?;
        Ok(QuicTransportConfig::builder()
            .max_idle_timeout(Some(it))
            .keep_alive_interval(Duration::from_mins(1))
            .max_concurrent_bidi_streams(VarInt::from_u32(100))
            .build())
    };

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let server_ep = Endpoint::builder(presets::Minimal)
        .secret_key(server_sk)
        .transport_config(build_cfg()?)
        .alpns(vec![ALPN_PROBE.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind_addr(server_bind)
        .map_err(|e| anyhow::anyhow!("server bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("server bind: {e}"))?;
    let server_addr = server_ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let server_addr = match server_addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };

    // Server task: accept the connection but don't drive any application
    // handler. We're testing transport-level idle close, not protocol
    // behaviour — the only thing that should close the connection is the
    // idle timer.
    let server_ep_bg = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        if let Some(incoming) = server_ep_bg.accept().await {
            let connecting = incoming
                .accept()
                .map_err(|e| anyhow::anyhow!("accept: {e}"))?;
            let conn = connecting
                .await
                .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
            let _ = conn.closed().await;
        }
        Ok::<_, anyhow::Error>(())
    });

    let client_bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let client_ep = Endpoint::builder(presets::Minimal)
        .secret_key(fresh_key())
        .transport_config(build_cfg()?)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(client_bind)
        .map_err(|e| anyhow::anyhow!("client bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("client bind: {e}"))?;

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let client_conn = client_ep
        .connect(target, ALPN_PROBE)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    // Wait for the idle timer to fire. The bound is generous compared to
    // `idle` so a slow CI doesn't flake the test, but tight enough that a
    // wiring regression (no `transport_config(...)` call, default 30s
    // timeout) fails fast instead of stalling for the full default.
    let close_err = tokio::time::timeout(Duration::from_secs(3), client_conn.closed())
        .await
        .map_err(|_| {
            anyhow::anyhow!("connection did not idle-close within 3s (idle window: {idle:?})")
        })?;

    match close_err {
        ConnectionError::TimedOut => {}
        other => anyhow::bail!("expected ConnectionError::TimedOut, got {other:?}"),
    }

    client_ep.close().await;
    accept_task
        .await
        .map_err(|e| anyhow::anyhow!("accept task join: {e}"))??;
    server_ep.close().await;
    Ok(())
}
