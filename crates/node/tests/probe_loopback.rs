//! Two-endpoint loopback test for `cdn/probe/v1`.
//!
//! Spawns a server endpoint running the probe handler, connects a client
//! endpoint over iroh on localhost, sends a `ProbeRequest`, and verifies the
//! response echoes the request, reports content availability, and carries a
//! valid EIP-712 `slash_sig` (ADR 005 / ADR 014, #318).

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::local::PrivateKeySigner;
use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
use decdn_cache::{CHUNK_GROUP_BYTES, CacheEngine, FilesystemOrigin, Hash};
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
    ALPN_PROBE, APP_ERR_RATE_LIMITED, Coverage, DISCOVERY_BLOCK_BYTES, MAX_MESSAGE_SIZE,
    ProbeMessage, ProbeResponseExt, SLASH_SIG_LEN, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse, ProbeResponseBody},
    num_blocks, parse_probe_response_ext, read_frame, write_frame,
};
use iroh::endpoint::{
    ApplicationClose, Connection, ConnectionError, IdleTimeout, QuicTransportConfig, ReadError,
    ReadToEndError, VarInt, presets,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};
use tokio::task::JoinHandle;
mod support;
use support::shutdown;

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

/// Open a cache whose STORE holds two blobs pulled through a single fs
/// origin, then removes `foreign_payload`'s object from that origin (leaving
/// only `own_payload` servable) and refreshes the origin-held index. Models
/// the origin-only policy's target shape: a store hit for content this node's
/// own origin can no longer serve (#1759).
async fn cache_with_own_and_foreign(
    own_payload: &[u8],
    foreign_payload: &[u8],
) -> anyhow::Result<(
    CacheEngine,
    Hash,
    Hash,
    tempfile::TempDir,
    tempfile::TempDir,
)> {
    let origin_dir = tempfile::tempdir()?;
    for payload in [own_payload, foreign_payload] {
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
    let (own_hash, foreign_hash) = (Hash::new(own_payload), Hash::new(foreign_payload));
    let _ = cache.get(own_hash).await?; // populate the local store
    let _ = cache.get(foreign_hash).await?;

    // Remove the foreign object from the fs origin: the store still holds it
    // (pulled above), but this node's own origin can no longer serve it — the
    // "store hit outside this node's own origin" shape the origin-only policy
    // must not advertise.
    let hex = foreign_hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    std::fs::remove_file(origin_dir.path().join(shard).join(hex.as_str()))?;
    cache.rescan_origins().await;

    Ok((cache, own_hash, foreign_hash, origin_dir, cache_dir))
}

/// Deterministic pseudo-random payload of `len` bytes (xorshift32), matching
/// the synthesis `decdn-cache`'s own coverage tests use — content doesn't
/// matter here, only that it hashes and bao-verifies consistently.
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, bytes::Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes()[0];
    }
    let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), plaintext, bytes::Bytes::from(ob.data))
}

/// Encode the header-less interleaved bao for `[off, off + len)` of a blob
/// with the given `root`/`plaintext`/`outboard`/`total` size — the same
/// `admit_bao`-ready shape `decdn-cache`'s own tests build, reimplemented
/// here off the crate's public `range_pull` helpers (the cache crate's own
/// `synth_blob`/`bao_for` are private test-only fns, not exported).
fn bao_for(
    root: [u8; 32],
    plaintext: &[u8],
    outboard: bytes::Bytes,
    off: u64,
    len: u64,
    total: u64,
) -> anyhow::Result<(Hash, bao_tree::ChunkRanges, bytes::Bytes)> {
    let aligned = align_range(off, len, total)?;
    let s = usize::try_from(aligned.fetch_start())?;
    let e = usize::try_from(aligned.fetch_end())?;
    let slice = plaintext
        .get(s..e)
        .ok_or_else(|| anyhow::anyhow!("aligned range out of bounds"))?;
    let encoded = encode_verified_range(root, &aligned, slice, outboard)?;
    Ok((Hash::from(root), aligned.chunk_ranges().clone(), encoded))
}

/// Open an origin-less cache and admit a **partial** two-discovery-block blob
/// into it: block 0 (`[0, DISCOVERY_BLOCK_BYTES)`) admitted in full, plus the
/// blob's trailing chunk group (which is what lets iroh-blobs learn the
/// `Partial` blob's total size — it only reports one once the FINAL chunk is
/// present), while block 1's middle group is left missing. This is the
/// "cached partial holder" shape #1506 exists to advertise: a store that
/// holds ≥1 discovery block but is not `Complete`, so the OLD
/// `Complete`-gated `has_blob` would report `false` for it.
async fn cache_with_partial_two_block_blob()
-> anyhow::Result<(CacheEngine, Hash, tempfile::TempDir)> {
    let cache_dir = tempfile::tempdir()?;
    let cache = CacheEngine::open(cache_dir.path(), vec![], 16).await?;

    let group = CHUNK_GROUP_BYTES;
    let total = DISCOVERY_BLOCK_BYTES + 3 * group;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);

    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard.clone(),
        0,
        DISCOVERY_BLOCK_BYTES,
        total,
    )?;
    cache.admit_bao(hash, block0_ranges, block0_bao).await?;

    let (_, tail_ranges, tail_bao) =
        bao_for(root, &plaintext, outboard, total - group, group, total)?;
    cache.admit_bao(hash, tail_ranges, tail_bao).await?;

    Ok((cache, hash, cache_dir))
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
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    // Most tests don't exercise the ADR 005 probe rate limiter — wire a
    // permissive one so only the layer under test (the `ConnectionLimiter`,
    // hold budget, etc.) can fire.
    build_handler_with_probe_limiter(
        server_id,
        rate,
        metrics,
        limiter,
        permissive_probe_rate_limiter(metrics),
        cache,
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
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        rate,
        Arc::clone(metrics),
        limiter,
        probe_limiter,
        cache,
        Arc::clone(&signer),
        domain.clone(),
        // No stake-lane reservation for the general-purpose builder; the
        // dedicated reservation tests use `build_handler_with_lane` (#757).
        None,
        // Relay foreign namespaces by default; the origin-only policy test
        // uses `build_handler_origin_only` (#1759).
        true,
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
        rate,
        Arc::clone(metrics),
        limiter,
        permissive_probe_rate_limiter(metrics),
        cache,
        Arc::clone(&signer),
        domain.clone(),
        Some(policy),
        true,
        None,
    ));
    (handler, signer, domain)
}

/// Build a `ProbeHandler` with `relay_foreign_namespaces = false` (ADR 002
/// origin-only node policy, #1759): the handler advertises backend-held
/// content only, regardless of what the store happens to hold.
#[allow(clippy::too_many_arguments)]
fn build_handler_origin_only(
    server_id: iroh::PublicKey,
    rate: u64,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
) -> (Arc<ProbeHandler>, Arc<PrivateKeySigner>, Eip712Domain) {
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        rate,
        Arc::clone(metrics),
        limiter,
        permissive_probe_rate_limiter(metrics),
        cache,
        Arc::clone(&signer),
        domain.clone(),
        None,
        false,
        None,
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
    build_handler_bounds(server_id, rate, metrics, limiter, cache)
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
    let (msg, tail) = decode_message::<ProbeMessage>(&frame)?;
    let (resp, resp_ext): (ProbeResponse, ProbeResponseExt) = match msg {
        ProbeMessage::Response(r) => (r, parse_probe_response_ext(tail)?),
        ProbeMessage::Request(_) => anyhow::bail!("unexpected request variant on client"),
    };

    assert_eq!(resp.body.timestamp_us, req.timestamp_us, "timestamp echoed");
    assert_eq!(resp.body.hash, req.hash, "hash echoed");
    assert_eq!(resp.body.rate_per_mb, rate_per_mb, "rate (unclamped)");
    assert!(
        !resp.body.has_blob,
        "empty cache must report has_blob=false"
    );
    assert_eq!(resp_ext.total_bytes, None, "no size when blob absent");
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    conn.close(0u32.into(), b"bye");
    shutdown([], [&client_ep]).await?;

    // Allow the server task to finish handling before closing its endpoint.
    support::reap("accept", accept_task).await??;
    shutdown([], [&server_ep]).await?;
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
    shutdown([], [&h.client_ep]).await?;
    // Handler is expected to return Err on these error-path tests; we only need
    // to confirm the task joined, not that it succeeded — bounded, so a handler
    // that parks fails here rather than parking the test.
    let _ = support::reap("accept", h.accept_task).await;
    shutdown([], [&h.server_ep]).await?;
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
    shutdown([], [&h.client_ep, &h.server_ep]).await?;
    Ok(())
}

/// Observe a rate-limit rejection's application close with a bounded retry,
/// requiring the `APP_ERR_RATE_LIMITED` (`0x10`) code **and** `expected_reason`
/// (the layer label, e.g. `"per-source"` / `"per_peer"`) at least once (#1594).
///
/// The ADR-005 reject path closes the connection with `0x10` + the layer-label
/// reason bytes, but QUIC's `CONNECTION_CLOSE` is best-effort and
/// unacknowledged: over loopback the server can tear the connection down before
/// that frame is delivered, and the client then observes an implicit code-0
/// close (quinn closes a dropped connection with code 0 / empty reason) or a
/// transport-level teardown instead. That is a rare (~1/several-thousand under
/// CI load) transport artifact, not a rate-limit-layer fault, so retry a fresh
/// rejected connection until the layer-labelled close is seen.
///
/// Requiring `expected_reason` keeps the test proving the reason-byte layering
/// it exists for: a `0x10` close carrying a *different* label is the also-`0x10`
/// global / per-IP cap and fails immediately (it is not a transient). `connect`
/// yields a fresh `(Endpoint, Connection)` per attempt; the endpoint is kept
/// alive across `closed()` and closed before the next attempt.
async fn observe_rate_limit_close<F, Fut>(
    expected_reason: &[u8],
    mut connect: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<(Endpoint, Connection)>>,
{
    const MAX_ATTEMPTS: usize = 8;
    let expected_code = VarInt::from_u32(APP_ERR_RATE_LIMITED);
    let mut last_transient = None;
    for _ in 0..MAX_ATTEMPTS {
        let (ep, conn) = connect().await?;
        let close_err = conn.closed().await;
        match close_err {
            ConnectionError::ApplicationClosed(ApplicationClose { error_code, reason })
                if error_code == expected_code =>
            {
                if reason.as_ref() == expected_reason {
                    shutdown([], [&ep]).await?;
                    return Ok(());
                }
                anyhow::bail!(
                    "rate-limit close carried the wrong layer label: expected {:?}, got {:?}",
                    String::from_utf8_lossy(expected_reason),
                    String::from_utf8_lossy(reason.as_ref()),
                );
            }
            // Transport race (#1594): an implicit code-0 close or a
            // transport-level teardown beat the reject frame over loopback.
            // Retry with a fresh connection.
            other => last_transient = Some(other),
        }
        shutdown([], [&ep]).await?;
    }
    anyhow::bail!(
        "never observed an APP_ERR_RATE_LIMITED close carrying {:?} in {MAX_ATTEMPTS} attempts; \
         last transient close: {last_transient:?}",
        String::from_utf8_lossy(expected_reason),
    )
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
/// Draining the bucket out of band is what keeps this deterministic: the test
/// opens no throwaway connection to charge the bucket, so nothing races the one
/// live connection's establishment, and the accept loop assumes no connection
/// count.
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
    // Accept in a loop, with no assumption about how many connections arrive:
    // `observe_rate_limit_close` may open more than one to ride out the rare
    // loopback close-frame race (#1594), so the server must service each.
    // Per-connection errors are swallowed so one transient teardown never tears
    // down the accept loop.
    let accept_task = tokio::spawn(async move {
        while let Some(incoming) = server_ep_bg.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else {
                continue;
            };
            let _ = handler_bg.accept(conn).await;
        }
    });

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    // Every connection is rejected by `ConnectionLimiter::acquire` at the top of
    // `serve` and closed with APP_ERR_RATE_LIMITED + the `per-source` layer
    // label. Observe with a bounded retry to absorb the #1594 close-frame race;
    // requiring the `per-source` reason bytes still proves the per-source layer
    // fired rather than the also-0x10 global cap.
    observe_rate_limit_close(RejectReason::PerSource.as_str().as_bytes(), move || {
        let target = target.clone();
        async move {
            let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
            // Close the just-bound endpoint on a connect error so a retried
            // attempt never leaks a socket/driver task (matches the file's
            // explicit-`close()` cleanup convention).
            match client_ep.connect(target, ALPN_PROBE).await {
                Ok(conn) => Ok((client_ep, conn)),
                Err(e) => {
                    shutdown([], [&client_ep]).await?;
                    Err(anyhow::anyhow!("connect: {e}"))
                }
            }
        }
    })
    .await?;

    // The live connection must have charged the *same* pre-drained key, not a
    // fresh one: a single tracked source confirms `peer_ip` resolved it to
    // 127.0.0.1 (a different key would have been admitted, not rejected). Every
    // retry binds a fresh client endpoint on 127.0.0.1, so the tracked-source
    // count stays 1 regardless of how many attempts the race required.
    assert_eq!(
        limiter_probe.per_source_tracked(),
        1,
        "live connection must hit the pre-drained 127.0.0.1 bucket"
    );

    shutdown([accept_task], [&server_ep]).await?;
    Ok(())
}

/// The requester side of the close the test above proves is emitted (#1986):
/// `probe_once` — the shared `cdn/probe/v1` client the node's cache-miss probe
/// fan-out and the CLI both use — must surface a `0x10` shed as the typed
/// [`UpstreamRateLimited`] sentinel carrying the layer label, recoverable with
/// `downcast_ref` through the stage context it adds. Without that the node's
/// `probe_candidate` sees a bare `open_bi failed` string and scores the shedding
/// peer `Unreachable`, the penalty the handler-level `Overloaded` refusal is
/// exonerated from.
///
/// Same strict per-source fixture as `probe_rate_limit_returns_rate_limited_close_code`,
/// and the same bounded retry over the #1594 close-frame race: an attempt whose
/// error is NOT the sentinel is retried on a fresh endpoint, and only a `0x10`
/// carrying the wrong label fails at once.
#[tokio::test(flavor = "multi_thread")]
async fn probe_once_types_a_rate_limit_close_as_upstream_rate_limited() -> anyhow::Result<()> {
    use decdn_client_pull::UpstreamRateLimited;
    use decdn_client_pull::probe::probe_once;

    const MAX_ATTEMPTS: usize = 8;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());

    let strict = ResolvedSecurity {
        max_concurrent_handlers: 64,
        per_source_rate_per_sec: 0.001,
        per_source_burst: 1,
        max_tracked_sources: 32,
    };
    let limiter = Arc::new(ConnectionLimiter::new(&strict, Arc::clone(&metrics)));
    drop(
        limiter
            .acquire_for_test(Some(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST)))
            .map_err(|r| anyhow::anyhow!("pre-drain unexpectedly rejected: {r:?}"))?,
    );

    let (cache, _cache_tmp) = empty_cache().await?;
    let (handler, _signer, _domain) = build_handler(server_id, 1, &metrics, limiter, cache);

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;
    let server_ep_bg = server_ep.clone();
    let handler_bg = Arc::clone(&handler);
    let accept_task = tokio::spawn(async move {
        while let Some(incoming) = server_ep_bg.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else {
                continue;
            };
            let _ = handler_bg.accept(conn).await;
        }
    });

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let mut last_transient = None;
    let mut observed = None;
    for _ in 0..MAX_ATTEMPTS {
        let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
        let res = probe_once(
            &client_ep,
            target.clone(),
            [0x11u8; 32],
            1_700_000_000_000_000,
            Duration::from_secs(5),
        )
        .await;
        shutdown([], [&client_ep]).await?;
        let Err(err) = res else {
            anyhow::bail!("a pre-drained per-source bucket must reject every probe");
        };
        match err.downcast_ref::<UpstreamRateLimited>() {
            Some(shed) => {
                observed = Some(shed.clone());
                break;
            }
            // Transport race (#1594): the reject frame lost to an implicit code-0
            // close or a transport teardown. Retry on a fresh connection.
            None => last_transient = Some(err),
        }
    }
    let shed = observed.ok_or_else(|| {
        anyhow::anyhow!(
            "never observed an UpstreamRateLimited probe error in {MAX_ATTEMPTS} attempts; \
             last transient error: {last_transient:?}"
        )
    })?;
    assert_eq!(
        shed.label.as_deref(),
        Some(RejectReason::PerSource.as_str()),
        "the sentinel must carry the per-source layer label off the close reason bytes"
    );

    shutdown([accept_task], [&server_ep]).await?;
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
/// then unconditionally rejected at the per-peer layer. Draining out of band
/// keeps this deterministic, the same rationale as
/// `probe_rate_limit_returns_rate_limited_close_code`.
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
    );

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_PROBE.to_vec()]).await?;
    let server_ep_bg = server_ep.clone();
    let handler_bg = Arc::clone(&handler);
    // Loop-accept for the same reason as the per-source test, and with the same
    // absence of a count assumption: the bounded retry in
    // `observe_rate_limit_close` may open more than one connection to ride out
    // the #1594 close-frame race.
    let accept_task = tokio::spawn(async move {
        while let Some(incoming) = server_ep_bg.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else {
                continue;
            };
            let _ = handler_bg.accept(conn).await;
        }
    });

    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    // Each connection is rejected at the ADR-005 per-peer layer and closed with
    // APP_ERR_RATE_LIMITED + the `per_peer` label. Every retry rebinds a fresh
    // client endpoint under the *same* pinned `client_sk`, so the requester
    // `NodeId` — and thus the pre-drained per-peer bucket — is unchanged and the
    // rejection is reproduced. Requiring the `per_peer` reason bytes proves the
    // per-peer layer fired (the gap #982 closed), not the also-0x10 per-IP or
    // global cap.
    observe_rate_limit_close(ProbeRejectLayer::PerPeer.as_str().as_bytes(), move || {
        let target = target.clone();
        let client_sk = client_sk.clone();
        async move {
            let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
            // Close the just-bound endpoint on a connect error so a retried
            // attempt never leaks a socket/driver task (matches the file's
            // explicit-`close()` cleanup convention).
            match client_ep.connect(target, ALPN_PROBE).await {
                Ok(conn) => Ok((client_ep, conn)),
                Err(e) => {
                    shutdown([], [&client_ep]).await?;
                    Err(anyhow::anyhow!("connect: {e}"))
                }
            }
        }
    })
    .await?;

    let scrape = metrics.encode().map_err(|e| anyhow::anyhow!("{e}"))?;
    // Normally exactly one per-peer rejection; the #1594 close-frame race can
    // make the observation retry, and every retried rejection also increments
    // this counter, so assert `>= 1` rather than a brittle exact count. The
    // reason-byte assertion above already proves it was the per-peer layer.
    let rejected = metric_value(&scrape, "decdn_probe_rate_limit_rejected_per_peer_total");
    anyhow::ensure!(
        rejected.is_some_and(|v| v >= 1),
        "probe per-peer rejection must appear in /metrics scrape:\n{scrape}"
    );

    shutdown([accept_task], [&server_ep]).await?;
    Ok(())
}

/// Drive one full probe exchange against `handler` on a loopback pair and
/// return the decoded response. Handles endpoint setup/teardown.
async fn run_one_probe(
    server_sk: SecretKey,
    handler: Arc<ProbeHandler>,
    req: ProbeRequest,
) -> anyhow::Result<(ProbeResponse, ProbeResponseExt)> {
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
) -> anyhow::Result<(ProbeResponse, ProbeResponseExt)> {
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
    let (msg, tail) = decode_message::<ProbeMessage>(&frame)?;
    let resp = match msg {
        ProbeMessage::Response(r) => (r, parse_probe_response_ext(tail)?),
        ProbeMessage::Request(_) => anyhow::bail!("unexpected request variant on client"),
    };
    conn.close(0u32.into(), b"bye");
    shutdown([], [&client_ep]).await?;
    support::reap("accept", accept_task).await??;
    shutdown([], [&server_ep]).await?;
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
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(resp.body.has_blob, "cached blob must report has_blob=true");
    anyhow::ensure!(
        resp_ext.total_bytes == Some(payload.len() as u64),
        "total_bytes should report the blob size, got {:?}",
        resp_ext.total_bytes
    );
    anyhow::ensure!(resp.body.hash == *hash.as_bytes(), "hash echoed");
    assert_slash_sig_valid(&resp, &signer, &domain)?;
    Ok(())
}

/// ADR 011 §Serving while chain-stale: a node holding the blob but whose chain
/// reads are stale answers `has_blob: false` — advertising it would be signed
/// slash evidence for a hash the node can no longer confirm is not taken down.
/// The freshness handle here is never stamped, so it reads stale, exactly as it
/// would after the grace window elapsed with no successful poll tick.
#[tokio::test(flavor = "multi_thread")]
async fn probe_has_blob_false_when_chain_stale() -> anyhow::Result<()> {
    let payload = b"probe content while the chain is unreachable";
    let (cache, hash, _cache_tmp) = cache_with_blob(payload).await?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let signer = Arc::new(PrivateKeySigner::random());
    let domain = test_slash_domain();
    // A never-stamped freshness handle reads stale (the boot enumeration and every
    // successful poll tick stamp it; neither has run here).
    let stale = decdn_node::chain_freshness::ChainFreshness::new(Duration::from_mins(30));
    let handler = Arc::new(ProbeHandler::new(
        server_id,
        7,
        Arc::clone(&metrics),
        limiter,
        permissive_probe_rate_limiter(&metrics),
        cache,
        Arc::clone(&signer),
        domain.clone(),
        None,
        true,
        Some(stale),
    ));

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0xabc_def,
    };
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        !resp.body.has_blob,
        "a chain-stale node must not advertise a held blob"
    );
    anyhow::ensure!(
        resp_ext.total_bytes.is_none(),
        "a suppressed advertisement carries no size, got {:?}",
        resp_ext.total_bytes
    );
    anyhow::ensure!(
        resp_ext.consistent_with(resp.body.has_blob),
        "has_blob and coverage.is_empty() must be a biconditional"
    );
    // Still a well-formed, signed response — this is a compliance posture, not a
    // fault.
    assert_slash_sig_valid(&resp, &signer, &domain)?;
    Ok(())
}

/// #1506: `has_blob` is redefined from "holds the whole blob" to "will serve
/// at least one discovery block". A node holding only discovery block 0 of a
/// two-block blob (never `Complete`, so the OLD `Complete`-gated `has_blob`
/// answered `false`) now signs `has_blob: true`, and the extension's
/// `coverage` matches the cache's own derivation exactly: block 0 covered,
/// block 1 not.
#[tokio::test(flavor = "multi_thread")]
async fn probe_has_blob_true_and_partial_coverage_for_cached_partial_holder() -> anyhow::Result<()>
{
    let (cache, hash, _cache_tmp) = cache_with_partial_two_block_blob().await?;
    // Cross-check against the cache's own derivation (`CacheEngine::coverage`) so
    // this test also catches the handler quietly diverging from it.
    let direct_coverage = cache.coverage(hash).await?;
    anyhow::ensure!(direct_coverage.covers(0) && !direct_coverage.covers(1));

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (handler, signer, domain) = build_handler(server_id, 7, &metrics, limiter, cache);

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0xf00d,
    };
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.has_blob,
        "a partial holder covering >=1 block must report has_blob=true (#1506), \
         not just a Complete holder"
    );
    anyhow::ensure!(
        resp_ext.coverage.covers(0),
        "block 0 was admitted in full and must be advertised as covered"
    );
    anyhow::ensure!(
        !resp_ext.coverage.covers(1),
        "block 1's middle group was never admitted and must not be advertised"
    );
    anyhow::ensure!(
        resp_ext.coverage == direct_coverage,
        "the handler's advertised coverage must match the cache's own derivation exactly"
    );
    anyhow::ensure!(
        resp_ext.consistent_with(resp.body.has_blob),
        "has_blob and coverage.is_empty() must be a biconditional"
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;
    Ok(())
}

/// #1506: an origin-serve-capable node (origin-held index or live origin size
/// probe) that holds NOTHING in its local cache signs `has_blob: true` with
/// all-ones `coverage` — an origin serves every block and already knows the
/// size. This is the origin all-ones case: distinct from the cached-partial
/// case above, and from the pre-#1506 behavior (which never populated
/// `coverage` at all).
#[tokio::test(flavor = "multi_thread")]
async fn probe_origin_held_advertises_full_coverage_without_caching() -> anyhow::Result<()> {
    let payload = b"origin-held content this node has never pulled into its own cache";
    let origin_dir = tempfile::tempdir()?;
    let hash = Hash::new(payload);
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
    // Deliberately no `cache.get(hash)` — the store must stay empty; presence
    // comes only from the fs-origin index (#1130).
    anyhow::ensure!(
        !cache.has(hash).await?,
        "the store must hold nothing for this test to exercise the origin-held path"
    );

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (handler, signer, domain) = build_handler(server_id, 7, &metrics, limiter, cache);

    let req = ProbeRequest {
        hash: *hash.as_bytes(),
        timestamp_us: 0xbeef,
    };
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.has_blob,
        "an origin-held blob must be advertised even though nothing is cached"
    );
    anyhow::ensure!(
        resp_ext.total_bytes == Some(payload.len() as u64),
        "origin content's total_bytes should report the backend size, got {:?}",
        resp_ext.total_bytes
    );
    anyhow::ensure!(
        resp_ext.coverage == Coverage::full(num_blocks(payload.len() as u64)),
        "an origin can serve every block, so coverage must be all-ones"
    );
    anyhow::ensure!(
        resp_ext.consistent_with(resp.body.has_blob),
        "has_blob and coverage.is_empty() must be a biconditional"
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;
    Ok(())
}

/// Origin-only node policy (ADR 002, #1759): with `relay_foreign_namespaces
/// = false`, the probe handler advertises exactly what its own backend
/// holds. A store hit for content outside the node's own origin — leftover
/// from before the toggle, or seeded some other way — must NOT be
/// advertised, because the serve gate now declines it and an advertise/decline
/// mismatch is the reputation hazard this policy closes. Own (fs-origin-held)
/// content is still advertised.
#[tokio::test(flavor = "multi_thread")]
async fn origin_only_probe_advertises_backend_only() -> anyhow::Result<()> {
    let own_payload = b"origin-only: own fs-origin content";
    let foreign_payload = b"origin-only: leftover store content, now foreign";
    let (cache, own_hash, foreign_hash, _origin_tmp, _cache_tmp) =
        cache_with_own_and_foreign(own_payload, foreign_payload).await?;

    let metrics = Arc::new(Metrics::new());
    let (handler, _signer, _domain) = build_handler_origin_only(
        fresh_key().public(),
        7,
        &metrics,
        permissive_limiter(&metrics),
        cache,
    );

    let foreign_req = ProbeRequest {
        hash: *foreign_hash.as_bytes(),
        timestamp_us: 1,
    };
    let (foreign_resp, foreign_resp_ext) =
        run_one_probe(fresh_key(), Arc::clone(&handler), foreign_req).await?;
    anyhow::ensure!(
        !foreign_resp.body.has_blob,
        "origin-only node must not advertise a store hit outside its own origin"
    );
    anyhow::ensure!(
        foreign_resp_ext.total_bytes.is_none(),
        "a non-advertised hash must carry no total_bytes hint, got {:?}",
        foreign_resp_ext.total_bytes
    );

    let own_req = ProbeRequest {
        hash: *own_hash.as_bytes(),
        timestamp_us: 2,
    };
    let (own_resp, own_resp_ext) = run_one_probe(fresh_key(), handler, own_req).await?;
    anyhow::ensure!(
        own_resp.body.has_blob,
        "origin-only node must still advertise its own backend-held content"
    );
    anyhow::ensure!(
        own_resp_ext.total_bytes == Some(own_payload.len() as u64),
        "own content's total_bytes should report the backend size, got {:?}",
        own_resp_ext.total_bytes
    );

    Ok(())
}

/// A node with the eviction-hold path **disabled by config**
/// (`max_probe_holds == 0`) holds the blob but answers `has_blob: false` with a
/// valid `slash_sig` over `has_blob=false`. The reason is the operator opt-out,
/// NOT the missing guarantee — `BudgetExhausted` also places no hold and still
/// advertises. `max_probe_holds = 0` means "do not advertise store-backed
/// content at all" (ADR 005 §Hold budget).
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
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        !resp.body.has_blob,
        "holds-disabled must yield has_blob=false even though the blob is cached"
    );
    anyhow::ensure!(
        resp_ext.total_bytes.is_none(),
        "no size advertised when has_blob=false, got {:?}",
        resp_ext.total_bytes
    );
    // The signature must cover has_blob=false (not a stale true).
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    // An intentional disable must increment the disabled counter and leave
    // the budget-pressure counter at zero — otherwise the "increase
    // max_probe_holds" alert fires on a config the operator chose.
    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"disabled\"}"
        ) == Some(1),
        "holds-disabled probe must bump the reason=disabled child of \
         decdn_probe_hold_unavailable_total:\n{text}"
    );
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"exhausted\"}"
        ) == Some(0),
        "an intentional disable must NOT inflate the budget-pressure counter:\n{text}"
    );
    Ok(())
}

/// A node with a positive but fully-occupied hold budget still advertises
/// `has_blob: true` (the hold is best-effort — presence, not a guaranteed hold,
/// governs the answer) but places no hold, and counts the event as genuine
/// budget pressure (`reason="exhausted"`), NOT as a config disable (#739). This
/// is the signal whose alert remedy is "increase `max_probe_holds`".
#[tokio::test(flavor = "multi_thread")]
async fn probe_budget_exhausted_still_advertises_and_counts_exhausted() -> anyhow::Result<()> {
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
    // Kept so the test can prove the budget was respected, not just reported:
    // `has_blob` looks identical whether the second probe forwent the hold or
    // overran `max_probe_holds`.
    let cache_probe = cache.clone();

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let (handler, signer, domain) = build_handler(server_id, 7, &metrics, limiter, cache);

    let req = ProbeRequest {
        hash: *hb.as_bytes(),
        timestamp_us: 0x55,
    };
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.has_blob,
        "budget-exhausted must still advertise has_blob=true — the hold is forgone, not the answer"
    );
    anyhow::ensure!(
        resp_ext.total_bytes == Some(b.len() as u64),
        "the advertised blob must carry its size, got {:?}",
        resp_ext.total_bytes
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    // The budget is a hard ceiling: the advertised-but-unheld blob must not
    // have taken a second slot. Only `ha` is held.
    anyhow::ensure!(
        cache_probe.probe_hold_slots_used() == 1,
        "budget-exhausted must forgo the hold, leaving max_probe_holds=1 slot \
         in use; found {}",
        cache_probe.probe_hold_slots_used()
    );

    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"exhausted\"}"
        ) == Some(1),
        "genuine budget exhaustion must bump the reason=exhausted child of \
         decdn_probe_hold_unavailable_total:\n{text}"
    );
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"disabled\"}"
        ) == Some(0),
        "budget pressure must NOT be counted as a config disable:\n{text}"
    );
    Ok(())
}

/// With a stake-lane reservation configured (#757, ADR 003 §Admission and
/// Priority), a probe from a client that is NOT a registered operator still
/// advertises `has_blob: true` but places no eviction hold once hold usage
/// reaches the end-client ceiling — the reservation protects node-to-node hold
/// slots, it does not suppress an honest answer. Here `max_holds=1, reserved=1`
/// gives a ceiling of `0`, so the end-client is shed from the hold immediately
/// even though the blob is cached. The event is counted as a stake-lane
/// reservation — never as budget exhaustion (`reason="exhausted"`) or a config
/// disable (`reason="disabled"`), whose alerts have different remedies.
#[tokio::test(flavor = "multi_thread")]
async fn probe_end_client_reserved_out_still_advertises() -> anyhow::Result<()> {
    let payload = b"reserved-for-stake-lane content";
    let (cache, hash, _cache_tmp) = cache_with_blob(payload).await?;
    cache.set_max_probe_holds(1);
    // Kept so the test can inspect hold state after the probe. Asserting the
    // wire answer alone cannot distinguish "advertised, no hold" from
    // "advertised AND consumed a reserved slot" — the latter is exactly the
    // regression #757 exists to prevent, and it is invisible in `has_blob`.
    let cache_probe = cache.clone();

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
    let (resp, resp_ext) = run_one_probe(server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.has_blob,
        "an end-client under a stake-lane reservation must still get has_blob=true (hold shed, not the answer)"
    );
    anyhow::ensure!(
        resp_ext.total_bytes == Some(payload.len() as u64),
        "the advertised blob must carry its size, got {:?}",
        resp_ext.total_bytes
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    // The point of the reservation: the end-client got an honest answer but
    // consumed NO hold slot, leaving the whole budget for the stake lane.
    anyhow::ensure!(
        cache_probe.probe_hold_slots_used() == 0,
        "a reserved-out end-client must place no hold; {} slot(s) in use",
        cache_probe.probe_hold_slots_used()
    );

    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"stake_lane_reserved\"}"
        ) == Some(1),
        "reservation refusal must bump the reason=stake_lane_reserved child of \
         decdn_probe_hold_unavailable_total:\n{text}"
    );
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"exhausted\"}"
        ) == Some(0),
        "a stake-lane reservation must NOT be counted as budget pressure:\n{text}"
    );
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"disabled\"}"
        ) == Some(0),
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
    // The mirror of the end-client assertion: a stake-lane requester must
    // actually CONSUME the slot the reservation held open for it.
    let cache_probe = cache.clone();

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
    let (resp, resp_ext) = run_one_probe_as(client_sk, server_sk, handler, req).await?;

    anyhow::ensure!(
        resp.body.has_blob,
        "a stake-lane requester must keep its reserved headroom -> has_blob=true"
    );
    anyhow::ensure!(
        resp_ext.total_bytes == Some(payload.len() as u64),
        "stake-lane requester should be served the blob size, got {:?}",
        resp_ext.total_bytes
    );
    assert_slash_sig_valid(&resp, &signer, &domain)?;

    // Unlike the shed end-client, this requester went through the real hold
    // path and took the reserved slot. Together the two tests pin the
    // reservation's actual behaviour, not just its counter.
    anyhow::ensure!(
        cache_probe.probe_hold_slots_used() == 1,
        "a stake-lane requester must consume its reserved hold slot; {} in use",
        cache_probe.probe_hold_slots_used()
    );

    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_value(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"stake_lane_reserved\"}"
        ) == Some(0),
        "a stake-lane requester must NOT trip the reservation counter, and the \
         series must be present at zero rather than absent:\n{text}"
    );
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

    shutdown([], [&client_ep]).await?;
    support::reap("accept", accept_task).await??;
    shutdown([], [&server_ep]).await?;
    Ok(())
}
