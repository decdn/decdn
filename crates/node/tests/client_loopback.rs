//! Two-endpoint loopback tests for `cdn/client/v1` paid delivery (#317).
//!
//! Spins up a server running [`ClientHandler`], connects a client endpoint over
//! iroh on localhost, and drives the [`stream_fetch`] requester against it. The
//! happy path proves bytes are delivered + hash-verified and the persisted
//! channel state advances; the error paths prove a zero-rate response is
//! rejected on receive (#252) and an unknown channel is cleanly rejected with
//! `VoucherRejected { WrongChannel }` (the #327 boundary).
//!
//! Delivery/authorization gates also covered: `BlobTooLarge` (size gate),
//! `EvictedSinceProbe` (evicted between probe and stream), the client-binding
//! mismatch reset and the binding-does-not-own-channel `NotFound` (both driven
//! by a small [`raw_request`] client, since the honest requester never sends a
//! binding), and per-channel voucher serialization under concurrency.
//!
//! Still uncovered (need a hostile client that reimplements the receive loop,
//! tracked as follow-ups): a mid-stream underpaying voucher → stream fails; a
//! `BadSignature`/`StaleNonce` rejection *after* an accepted voucher; the
//! per-connection stream-cap reset-without-signing; a server over-sending or
//! delivering hash-mismatched bytes; and the request/voucher read timeouts.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, FilesystemOrigin, Hash};
use decdn_common::config::ResolvedSecurity;
use decdn_incentive::{
    ChannelState, ChannelStateStore, EPHEMERAL_BINDING_NONCE, MemoryChannelStateStore,
    bind_node_id_domain, binding_signing_hash, slash_judge_domain, voucher_domain,
};
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::client::ClientHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::client::{ClientBinding, ClientMessage, StreamRequest, StreamRequestExt};
use decdn_protocol::{
    ALPN_CLIENT, MAX_RATE_PER_MB, decode_message, encode_stream_request, read_frame, write_frame,
};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, endpoint::presets};

const CHAIN_ID: u64 = 421_614;
const TOKEN: Address = Address::repeat_byte(0x22);
const RATE_PER_MB: u64 = 10;

fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

fn slash_domain() -> Eip712Domain {
    slash_judge_domain(CHAIN_ID, Address::repeat_byte(0x11))
}

fn payment_domain() -> Eip712Domain {
    voucher_domain(CHAIN_ID, Address::repeat_byte(0x34))
}

fn binding_domain() -> Eip712Domain {
    bind_node_id_domain(CHAIN_ID, Address::repeat_byte(0x99))
}

const fn channel_id() -> B256 {
    B256::repeat_byte(0xC1)
}

async fn empty_cache() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), vec![], 16).await?;
    Ok((cache, tmp))
}

/// Open a cache pre-seeded with `payload` (pulled+verified via a filesystem
/// origin, then the origin dir is dropped). Returns the cache, blob hash, and
/// the cache temp dir to keep alive.
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
    let _ = cache.get(hash).await?; // populate local store
    drop(origin_dir);
    Ok((cache, hash, cache_dir))
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

/// Build a `ClientHandler` with the given rate and channel store, an unlimited
/// blob-size gate, and a 16-stream per-connection cap (the common-case setup).
fn build_handler(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
) -> anyhow::Result<Arc<ClientHandler>> {
    build_handler_limited(
        server_id, server_eth, metrics, limiter, cache, store, rate, 0, 16,
    )
}

/// Build a `ClientHandler` exposing the `max_blob_size_bytes` and
/// `max_concurrent_streams` knobs (`0` blob size == unlimited).
#[allow(clippy::too_many_arguments)]
fn build_handler_limited(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
) -> anyhow::Result<Arc<ClientHandler>> {
    Ok(Arc::new(ClientHandler::new(
        server_id,
        Arc::clone(metrics),
        limiter,
        cache,
        Arc::clone(server_eth),
        slash_domain(),
        payment_domain(),
        binding_domain(),
        store,
        Arc::new(AtomicU64::new(rate)),
        0,
        MAX_RATE_PER_MB,
        1, // voucher_interval_mb
        max_blob_size_bytes,
        max_concurrent_streams,
    )?))
}

/// Spawn a server endpoint running `handler`, accepting connections until the
/// endpoint closes.
fn spawn_server(server_ep: Endpoint, handler: Arc<ClientHandler>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = server_ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let _ = handler.accept(conn).await;
        }
    })
}

fn channel_context(client_signer: Arc<PrivateKeySigner>, deposit: U256) -> ChannelContext {
    ChannelContext {
        channel_id: channel_id(),
        token: TOKEN,
        deposit,
        client_signer,
        voucher_domain: payment_domain(),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
    }
}

/// Full happy path: a 1.5 MiB blob crosses one voucher-interval boundary plus a
/// closing voucher (two vouchers), the requester hash-verifies the bytes, and
/// the persisted channel state advances to nonce 2 / full byte count.
#[tokio::test(flavor = "multi_thread")]
async fn client_delivery_roundtrip_advances_channel_state() -> anyhow::Result<()> {
    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(Arc::clone(&client_signer), deposit);

    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await?;

    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "delivered bytes mismatch"
    );

    // The persisted channel advanced: two vouchers (interval + closing), full
    // byte count, non-zero cumulative amount.
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_nonce == U256::from(2u64),
        "nonce: {}",
        only.last_nonce
    );
    anyhow::ensure!(
        only.last_bytes_delivered == U256::from(payload.len()),
        "bytes_delivered: {}",
        only.last_bytes_delivered
    );
    anyhow::ensure!(only.last_amount > U256::ZERO, "amount must be non-zero");

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// Reused channel: two sequential streams on one channel. The second stream
/// must resume from the first's cumulative voucher state (nonce/bytes/amount),
/// not restart at zero — otherwise the node rejects the second voucher as
/// `StaleNonce`/`BytesRegression`. Validates the `ChannelContext.prior_*`
/// resume fields.
#[tokio::test(flavor = "multi_thread")]
async fn client_reused_channel_resumes() -> anyhow::Result<()> {
    let payload = vec![0x33u8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Stream 1: fresh channel (prior_* = 0).
    let ctx1 = channel_context(Arc::clone(&client_signer), deposit);
    let got1 = stream_fetch(
        &client_ep,
        target.clone(),
        &ctx1,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x1111,
        Duration::from_secs(15),
    )
    .await?;
    anyhow::ensure!(got1.as_ref() == payload.as_slice());

    let after1 = store.load_all()?;
    let s1 = after1
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        s1.last_nonce == U256::from(1u64),
        "nonce after 1: {}",
        s1.last_nonce
    );
    anyhow::ensure!(s1.last_bytes_delivered == U256::from(payload.len()));

    // Stream 2: resume from the channel's advanced state.
    let ctx2 = ChannelContext {
        prior_nonce: s1.last_nonce,
        prior_bytes_delivered: s1.last_bytes_delivered,
        prior_amount: s1.last_amount,
        ..channel_context(Arc::clone(&client_signer), deposit)
    };
    let got2 = stream_fetch(
        &client_ep,
        target,
        &ctx2,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x2222,
        Duration::from_secs(15),
    )
    .await?;
    anyhow::ensure!(got2.as_ref() == payload.as_slice());

    // The channel advanced again: nonce 2, cumulative bytes = both streams.
    let after2 = store.load_all()?;
    let s2 = after2
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        s2.last_nonce == U256::from(2u64),
        "nonce after 2: {}",
        s2.last_nonce
    );
    anyhow::ensure!(
        s2.last_bytes_delivered == U256::from(2 * payload.len()),
        "cumulative bytes: {}",
        s2.last_bytes_delivered
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// `byte_offset` resumes from a partial position: the requester receives only
/// the suffix and the node prices only the delivered bytes.
#[tokio::test(flavor = "multi_thread")]
async fn client_byte_offset_returns_suffix() -> anyhow::Result<()> {
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(Arc::clone(&client_signer), deposit);

    let offset = 1000u64;
    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        offset,
        0xfeed,
        Duration::from_secs(15),
    )
    .await?;

    let off = usize::try_from(offset)?;
    let suffix = payload
        .get(off..)
        .ok_or_else(|| anyhow::anyhow!("offset past payload"))?;
    anyhow::ensure!(
        got.as_ref() == suffix,
        "offset fetch must return the suffix"
    );
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_bytes_delivered == U256::from(suffix.len()),
        "bytes_delivered should be the suffix length, got {}",
        only.last_bytes_delivered
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// #252: the requester rejects a `rate_per_mb == 0` response on receive. A
/// handler configured with rate 0 (and floor 0) signs a zero-rate response;
/// `stream_fetch` must refuse it before paying anything.
#[tokio::test(flavor = "multi_thread")]
async fn client_rejects_zero_rate_response() -> anyhow::Result<()> {
    let payload = b"zero-rate must be refused on receive".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    // rate 0 → the response advertises rate_per_mb = 0.
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        0,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(client_signer, deposit);

    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x1234,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("zero-rate response must be rejected"))?;
    anyhow::ensure!(
        err.to_string().contains("rate") || err.to_string().contains("zero"),
        "error should mention the zero rate: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// #327 boundary: a voucher for a channel the node has never persisted is
/// rejected mid-stream with `VoucherRejected { WrongChannel }`, delivered
/// cleanly so the client reads the reason (no QUIC reset).
#[tokio::test(flavor = "multi_thread")]
async fn client_unknown_channel_is_rejected() -> anyhow::Result<()> {
    let payload = b"unknown channel pays nothing".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    // Empty store: the channel is unknown to the node.
    let store = Arc::new(MemoryChannelStateStore::new());

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(client_signer, U256::from(10_000_000u64));

    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x5678,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("unknown channel must be rejected"))?;
    anyhow::ensure!(
        err.to_string().contains("WrongChannel") || err.to_string().contains("rejected"),
        "error should surface the voucher rejection: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// A channel store seeded with one channel owned by a fresh client signer.
/// Returns the store, that client signer, and the deposit.
fn seeded_store() -> anyhow::Result<(Arc<dyn ChannelStateStore>, Arc<PrivateKeySigner>, U256)> {
    let signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        signer.address(),
        TOKEN,
        deposit,
    ))?;
    Ok((store, signer, deposit))
}

/// Spin up a server endpoint running a `ClientHandler` over `cache`/`store`,
/// returning the dialable target, the node's Ethereum signer (for `slash_sig`
/// verification), the server endpoint, and its accept-loop handle. `max_blob`
/// of `0` disables the size gate.
async fn spawn_handler_server(
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
    max_blob: u64,
    max_streams: usize,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<PrivateKeySigner>,
    Endpoint,
    tokio::task::JoinHandle<()>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_limited(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        rate,
        max_blob,
        max_streams,
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth, server_ep, server_task))
}

/// Minimal raw client for the binding paths the honest `stream_fetch` requester
/// never drives (it always sends `ext = None`): open one bidi stream, send a
/// `StreamRequest` plus the given `ext`, and return the first decoded reply.
/// Errors if the server reset/closed the stream before a frame arrived — which
/// is exactly how a binding-verification failure surfaces to the peer.
async fn raw_request(
    client_ep: &Endpoint,
    target: EndpointAddr,
    req: &StreamRequest,
    ext: Option<&StreamRequestExt>,
) -> anyhow::Result<ClientMessage> {
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let payload = encode_stream_request(req, ext)?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read frame (stream reset?): {e}"))?;
    let (msg, _rest) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    Ok(msg)
}

/// Sign a `BindNodeId(client_node_id, nonce = EPHEMERAL_BINDING_NONCE)` under
/// the server's binding domain, returning the wire signature bytes.
fn sign_binding_for(signer: &PrivateKeySigner, client_node_id: B256) -> anyhow::Result<Vec<u8>> {
    let hash = binding_signing_hash(client_node_id, EPHEMERAL_BINDING_NONCE, &binding_domain());
    Ok(signer.sign_hash_sync(&hash)?.as_bytes().to_vec())
}

/// Size gate: a blob larger than `max_blob_size_bytes` is refused with
/// `ok: false` / `BlobTooLarge` before any bytes (or payment) flow.
#[tokio::test(flavor = "multi_thread")]
async fn client_blob_too_large_is_refused() -> anyhow::Result<()> {
    let payload = vec![0x7Eu8; 8192];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    // max_blob_size 4096 < 8192-byte payload → the size gate refuses delivery.
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 4096, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x00b1,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("oversized blob must be refused"))?;
    anyhow::ensure!(
        err.to_string().contains("BlobTooLarge") || err.to_string().contains("refused"),
        "error should surface BlobTooLarge: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// A blob evicted between probe and stream request is refused with
/// `EvictedSinceProbe` (distinct from never-had-it `NotFound`).
#[tokio::test(flavor = "multi_thread")]
async fn client_evicted_since_probe_is_refused() -> anyhow::Result<()> {
    let payload = b"evicted between probe and stream".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    cache.evict(hash)?; // logically gone: has() now false, is_evicted() true.
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x00e1,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("evicted blob must be refused"))?;
    anyhow::ensure!(
        err.to_string().contains("EvictedSinceProbe") || err.to_string().contains("refused"),
        "error should surface EvictedSinceProbe: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// Client-identity binding gate (ADR 005 §Client identity binding): a binding
/// whose signature recovers a DIFFERENT address than it claims is a client
/// fault — the node resets the stream (no signed response), so the raw read
/// fails. Covers `serve_stream`'s `recovered != claimed` arm.
#[tokio::test(flavor = "multi_thread")]
async fn client_binding_address_mismatch_resets() -> anyhow::Result<()> {
    let payload = b"binding mismatch never delivers".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, _owner, _deposit) = seeded_store()?;
    let (target, _server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    // A valid signature by `attacker`, but the binding CLAIMS `claimed`'s address.
    let attacker = PrivateKeySigner::random();
    let claimed = PrivateKeySigner::random();
    let claimed_addr: [u8; 20] = claimed.address().into();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: claimed_addr,
            binding_signature: sign_binding_for(&attacker, client_node_id)?,
        }),
        ..Default::default()
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        channel_id: channel_id().into(),
        byte_offset: 0,
        timestamp_us: 0x00ba_d001,
    };
    let res = raw_request(&client_ep, target, &req, Some(&ext)).await;
    anyhow::ensure!(
        res.is_err(),
        "a binding recovering a different address must reset the stream, got {res:?}"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// Binding authorization gate: a correctly-signed binding for an address that
/// does NOT own the requested channel is refused with `NotFound` (the leech
/// closure for bound clients). Covers `serve_stream`'s `client != owner` arm.
#[tokio::test(flavor = "multi_thread")]
async fn client_binding_for_other_owner_is_not_found() -> anyhow::Result<()> {
    let payload = b"valid binding, wrong channel owner".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    // The channel is owned by `seeded_store`'s signer; the binding attests a
    // different address (`intruder`), so it must not authorize this channel.
    let (store, _owner, _deposit) = seeded_store()?;
    let (target, _server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let intruder = PrivateKeySigner::random();
    let intruder_addr: [u8; 20] = intruder.address().into();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: intruder_addr,
            binding_signature: sign_binding_for(&intruder, client_node_id)?,
        }),
        ..Default::default()
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        channel_id: channel_id().into(),
        byte_offset: 0,
        timestamp_us: 0x00ba_d002,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        ClientMessage::StreamResponse(resp) => {
            anyhow::ensure!(
                !resp.body.ok,
                "expected ok:false for an unauthorized binding"
            );
            anyhow::ensure!(
                matches!(
                    resp.error,
                    Some(decdn_protocol::client::StreamError::NotFound)
                ),
                "expected NotFound, got {:?}",
                resp.error
            );
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// Per-channel serialization: two un-coordinated streams on one channel both
/// start from `prior_nonce = 0`, so both sign voucher nonce 1. The per-channel
/// mutex must serialize application so EXACTLY ONE is accepted (the other is a
/// stale nonce) — guarding against a lost-update / double-accept race that
/// would let a second voucher overwrite the first at the same nonce.
#[tokio::test(flavor = "multi_thread")]
async fn client_concurrent_same_channel_accepts_one_voucher() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, Arc::clone(&store), RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx_a = channel_context(Arc::clone(&signer), deposit);
    let ctx_b = channel_context(Arc::clone(&signer), deposit);
    let server_addr = server_eth.address();
    let sd = slash_domain();
    let (ra, rb) = tokio::join!(
        stream_fetch(
            &client_ep,
            target.clone(),
            &ctx_a,
            &sd,
            server_addr,
            *hash.as_bytes(),
            0,
            0x00aa,
            Duration::from_secs(15),
        ),
        stream_fetch(
            &client_ep,
            target.clone(),
            &ctx_b,
            &sd,
            server_addr,
            *hash.as_bytes(),
            0,
            0x00bb,
            Duration::from_secs(15),
        ),
    );
    let oks = usize::from(ra.is_ok()) + usize::from(rb.is_ok());
    anyhow::ensure!(
        oks == 1,
        "exactly one concurrent voucher must be accepted, got {oks} ok (a={ra:?}, b={rb:?})"
    );

    // State advanced exactly once: nonce 1, one stream's bytes, no double-apply.
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_nonce == U256::from(1u64),
        "nonce: {}",
        only.last_nonce
    );
    anyhow::ensure!(
        only.last_bytes_delivered == U256::from(payload.len()),
        "bytes_delivered: {}",
        only.last_bytes_delivered
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// A node without the blob answers `ok: false` with `NotFound`; the requester
/// surfaces the refusal without paying.
#[tokio::test(flavor = "multi_thread")]
async fn client_not_found_is_refused() -> anyhow::Result<()> {
    let (cache, _cache_tmp) = empty_cache().await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(client_signer, deposit);

    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        [0x42u8; 32], // a hash the node does not hold
        0,
        0x9abc,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("absent blob must be refused"))?;
    anyhow::ensure!(
        err.to_string().contains("refused") || err.to_string().contains("NotFound"),
        "error should surface the delivery refusal: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}
