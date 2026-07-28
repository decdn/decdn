//! Two-endpoint loopback tests for `cdn/client/v1` paid delivery (#317).
//!
//! Spins up a server running [`ClientHandler`], connects a client endpoint over
//! iroh on localhost, and drives the [`stream_fetch`] requester against it. The
//! happy path proves bytes are delivered + hash-verified and the persisted
//! channel state advances; the error paths prove a zero-rate response is
//! rejected on receive (#252), an unknown channel is cleanly rejected with
//! `VoucherRejected { WrongChannel }` (the #327 boundary), and a transient
//! persist-write failure is cleanly rejected with `VoucherRejected { RetryLater }`
//! (ADR 003 §332) rather than dropping the connection.
//!
//! Delivery/authorization gates also covered: `BlobTooLarge` (size gate),
//! `EvictedSinceProbe` (evicted between probe and stream), the client-binding
//! mismatch reset and the binding-does-not-own-channel `NotFound` (both driven
//! by a small [`raw_request`] client, since the honest requester never sends a
//! binding), and per-channel voucher serialization under concurrency.
//!
//! The ADR 005 §Connection lifetime idle-close (#1193) has its own group: the
//! never-opened-a-stream reap, the `inflight.is_empty()` gate under a parked
//! request read, and — driven by [`stall_delivery_at_closing_voucher`], a raw
//! delivery client that parks a REAL paid stream at its closing-voucher exchange
//! — the gate under active deliveries, repeated clock re-arms, and the rule that
//! every concurrent stream must finish before the idle countdown begins (#1261,
//! #1287).
//!
//! Still uncovered (need a hostile client that reimplements the receive loop —
//! [`stall_delivery_at_closing_voucher`] is now that client's honest half and is
//! the obvious base to extend; tracked as follow-ups): a mid-stream underpaying
//! voucher → stream fails; a `BadSignature`/`StaleNonce` rejection *after* an
//! accepted voucher; the per-connection stream-cap reset-without-signing; a
//! server over-sending or delivering hash-mismatched bytes; and the
//! request/voucher read timeouts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_cache::{
    CacheEngine, CacheMetrics, CircuitBreakerPolicy, Hash, PinnedHashes, RetryPolicy,
};
use decdn_incentive::{
    ChannelState, ChannelStateStore, CooperativeClose, EPHEMERAL_BINDING_NONCE,
    MemoryChannelStateStore, SignedCooperativeClose, Voucher, bind_node_id_domain,
    binding_signing_hash, min_payment, signed_to_wire_voucher, slash_judge_domain,
    stream_sig::StreamSlashData, voucher_domain,
};
use decdn_node::client_requester::{
    ChannelContext, ChannelLedger, Cumulative, PullDeadlines, RateAboveCeiling,
    UpstreamVoucherRejected, VoucherProgress, sign_client_binding, stream_fetch,
    stream_fetch_shared, stream_fetch_tracked, stream_fetch_tracked_with_progress,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::client::{ClientHandler, ClientHandlerDeps};
use decdn_node::metrics::Metrics;
use decdn_node::region_accounting::{RegionAccountant, RegionResolver, UNKNOWN_REGION};
use decdn_protocol::client::{
    ClientBinding, ClientMessage, CooperativeCloseRequest, StreamRequest, StreamRequestExt,
    VoucherRejectReason,
};
use decdn_protocol::{
    ALPN_CLIENT, decode_message, encode_message, encode_stream_request, read_frame, write_frame,
};
use iroh::endpoint::{Connection, ConnectionError, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

mod support;
use decdn_node::receipt_log::{DownloadReceipt, spawn_receipt_writer};
use support::{
    BlockingReceiptLog, FailingReceiptLog, HandlerDomains, VecReceiptLog, build_handler_full,
    build_handler_full_configured, build_handler_full_with_receipts, build_handler_full_with_sink,
    cache_with_blob, empty_cache, fresh_key, local_endpoint, permissive_limiter, read_client_msg,
    spawn_server, write_client_msg,
};
use tokio_util::sync::CancellationToken;

const CHAIN_ID: u64 = 421_614;
const TOKEN: Address = Address::repeat_byte(0x22);
const RATE_PER_MB: u64 = 10;

/// Lower-hex encode bytes (no `0x`), matching `DownloadReceipt`'s rendering so a
/// test can reconstruct the expected `hash` / `client_node_id` strings.
fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
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

/// The loopback suite's fixed EIP-712 domains.
fn loopback_domains() -> HandlerDomains {
    HandlerDomains {
        slash: slash_domain(),
        voucher: payment_domain(),
        binding: binding_domain(),
    }
}

const fn channel_id() -> B256 {
    B256::repeat_byte(0xC1)
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
    build_handler_full(
        server_id,
        server_eth,
        metrics,
        limiter,
        cache,
        store,
        rate,
        &loopback_domains(),
        max_blob_size_bytes,
        max_concurrent_streams,
    )
}

/// [`build_handler`] with a construction-time `configure` hook for the optional
/// deps that tests used to `attach_*` onto the built handler (#1254).
#[allow(clippy::too_many_arguments)]
fn build_handler_configured(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
    configure: impl FnOnce(&mut ClientHandlerDeps),
) -> anyhow::Result<Arc<ClientHandler>> {
    build_handler_limited_configured(
        server_id, server_eth, metrics, limiter, cache, store, rate, 0, 16, configure,
    )
}

/// [`build_handler_limited`] with a construction-time `configure` hook (#1254).
#[allow(clippy::too_many_arguments)]
fn build_handler_limited_configured(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
    configure: impl FnOnce(&mut ClientHandlerDeps),
) -> anyhow::Result<Arc<ClientHandler>> {
    build_handler_full_configured(
        server_id,
        server_eth,
        metrics,
        limiter,
        cache,
        store,
        rate,
        &loopback_domains(),
        max_blob_size_bytes,
        max_concurrent_streams,
        configure,
    )
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
        client_binding: None,
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
        only.last_nonce() == U256::from(2u64),
        "nonce: {}",
        only.last_nonce()
    );
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(wire),
        "bytes_delivered: {} (expected {wire})",
        only.last_bytes_delivered()
    );
    anyhow::ensure!(only.last_amount() > U256::ZERO, "amount must be non-zero");

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// ADR 005 §Connection lifetime (#1193): a connection with no active stream is
/// closed by the application layer after `APP_IDLE_TIMEOUT`. A short timeout is
/// injected via `ClientHandlerDeps.idle_timeout` so the test need not wait the production 30s.
/// The client opens no stream, so the server's serve loop is idle from the start
/// and must close it; the close is asserted to be the graceful no-error "idle"
/// close (`APP_ERR_NO_ERROR` + reason `"idle"`), not a fault or transport reset.
#[tokio::test(flavor = "multi_thread")]
async fn idle_connection_is_closed_by_the_app_layer() -> anyhow::Result<()> {
    let (cache, _cache_tmp) = empty_cache().await?;
    let store: Arc<dyn ChannelStateStore> = Arc::new(MemoryChannelStateStore::new());

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        |deps| deps.idle_timeout = Some(Duration::from_millis(300)),
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    // No stream is ever opened: the serve loop is idle from the first poll and
    // must close us after the injected 300ms window. The outer 10s bound is
    // generous against CI jitter yet still fails if the idle arm never fires.
    let err = tokio::time::timeout(Duration::from_secs(10), conn.closed())
        .await
        .map_err(|_| anyhow::anyhow!("connection was not idle-closed within 10s"))?;

    ensure_graceful_idle_close(&err)?;

    // The reap must be metered, not just logged at `debug!` — the counter is the
    // operator's only signal for the streamless-keep-alive abuse pattern (#1193).
    // Asserting the exact `name value` line also guards the recorder wiring: a
    // typo'd metric name or a bump of the wrong counter would fail here.
    let encoded = metrics.encode()?;
    anyhow::ensure!(
        metric_line_present(&encoded, "decdn_client_idle_close_total 1"),
        "idle-close must bump decdn_client_idle_close_total; got:\n{encoded}"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// ADR 005 §Connection lifetime (#1193): the app-layer idle reaper must (a) NOT
/// close a connection while a stream is still being served, and (b) re-arm from
/// that stream's completion — the two invariants the never-opened-a-stream test
/// above cannot reach. The client opens one stream and parks it mid-request (it
/// sends only the frame's length prefix, so the server blocks in `read_frame`),
/// holding the serve loop's `inflight` non-empty; the connection must survive
/// several idle windows. Completing the frame lets the serve future finish and
/// empties `inflight`; the reaper then re-arms and reaps the now-idle connection.
#[tokio::test(flavor = "multi_thread")]
async fn in_flight_stream_defers_idle_close_then_reaps_on_completion() -> anyhow::Result<()> {
    // Frame-body length promised in the prefix but withheld to park the server's
    // `read_frame`; a single varint byte since 64 < 128.
    const BODY_LEN: u8 = 64;

    let (cache, _cache_tmp) = empty_cache().await?;
    let store: Arc<dyn ChannelStateStore> = Arc::new(MemoryChannelStateStore::new());

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let idle = Duration::from_millis(150);
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        |deps| deps.idle_timeout = Some(idle),
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    // Open a stream and send ONLY the frame's length prefix, then withhold the
    // body. The server accepts the stream and parks in `read_frame`'s
    // `read_exact`, so its serve future stays in `inflight` — held there well
    // within the 5s request-read timeout.
    let (mut send, _recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    send.write_all(&[BODY_LEN])
        .await
        .map_err(|e| anyhow::anyhow!("write frame len: {e}"))?;

    // (a) Gating: with a stream in flight the reaper is disabled, so the
    // connection must NOT be closed even after several idle windows. A resolved
    // `closed()` here means the `inflight.is_empty()` gate is broken and a live
    // stream was truncated. The `idle * 5` bound is well past the idle window yet
    // far short of the 5s read timeout, so a pass is the gate holding open, not
    // the parked read expiring.
    let premature = tokio::time::timeout(idle * 5, conn.closed()).await;
    anyhow::ensure!(
        premature.is_err(),
        "connection was idle-closed while a stream was in flight: {premature:?}"
    );

    // Complete the frame; the 64 bytes decode to no valid request, so the server
    // resets the stream and its serve future returns, emptying `inflight`.
    send.write_all(&[0xFFu8; BODY_LEN as usize])
        .await
        .map_err(|e| anyhow::anyhow!("write frame body: {e}"))?;

    // (b) Re-arm: from that completion the idle clock restarts and must reap the
    // now-streamless connection with the same graceful no-error "idle" close.
    let err = tokio::time::timeout(Duration::from_secs(10), conn.closed())
        .await
        .map_err(|_| {
            anyhow::anyhow!("connection was not idle-closed after the stream completed")
        })?;
    ensure_graceful_idle_close(&err)?;

    // Exactly one connection was reaped, and only after the stream finished.
    let encoded = metrics.encode()?;
    anyhow::ensure!(
        metric_line_present(&encoded, "decdn_client_idle_close_total 1"),
        "post-stream idle-close must bump decdn_client_idle_close_total; got:\n{encoded}"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// One cached blob exposed by an [`IdleFixture`].
#[derive(Clone, Copy)]
struct IdleBlob {
    hash: Hash,
    /// Bao wire bytes the whole-blob delivery emits (content plus interleaved
    /// proof, ADR 038) — what a raw client must read before the server parks on
    /// the closing voucher, and what that voucher must cover.
    wire_bytes: u64,
}

/// A server serving one or more blobs under an injected idle window, plus
/// everything a raw client needs to drive paid deliveries against it — the
/// shared fixture for the #1261 and #1287 idle-close guard tests below.
struct IdleFixture {
    target: EndpointAddr,
    /// The channel's authorized client — the key a voucher must recover to.
    client_signer: Arc<PrivateKeySigner>,
    store: Arc<MemoryChannelStateStore>,
    blobs: Vec<IdleBlob>,
    metrics: Arc<Metrics>,
    server_ep: Endpoint,
    server_task: tokio::task::JoinHandle<()>,
    _cache_tmp: tempfile::TempDir,
}

impl IdleFixture {
    fn blob(&self, index: usize) -> anyhow::Result<IdleBlob> {
        self.blobs
            .get(index)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("idle fixture has no blob at index {index}"))
    }
}

/// Build an [`IdleFixture`] serving `payload` with `idle` injected as the
/// app-layer idle-close window (so a test need not wait the production 30s).
async fn idle_fixture(payload: &[u8], idle: Duration) -> anyhow::Result<IdleFixture> {
    let (cache, hash, cache_tmp) = cache_with_blob(payload).await?;

    idle_fixture_with_cache(
        cache,
        vec![IdleBlob {
            hash,
            wire_bytes: support::bao_wire_len_whole(payload.len() as u64),
        }],
        cache_tmp,
        idle,
    )
    .await
}

/// Build an [`IdleFixture`] serving two distinct blobs on the same connection.
async fn idle_fixture_with_two_blobs(
    a: &[u8],
    b: &[u8],
    idle: Duration,
) -> anyhow::Result<IdleFixture> {
    let (cache, hash_a, hash_b, cache_tmp) = cache_with_two_blobs(a, b).await?;
    idle_fixture_with_cache(
        cache,
        vec![
            IdleBlob {
                hash: hash_a,
                wire_bytes: support::bao_wire_len_whole(a.len() as u64),
            },
            IdleBlob {
                hash: hash_b,
                wire_bytes: support::bao_wire_len_whole(b.len() as u64),
            },
        ],
        cache_tmp,
        idle,
    )
    .await
}

/// Wire an idle-close handler around a pre-populated cache without duplicating
/// the loopback endpoint and payment-channel setup.
async fn idle_fixture_with_cache(
    cache: CacheEngine,
    blobs: Vec<IdleBlob>,
    cache_tmp: tempfile::TempDir,
    idle: Duration,
) -> anyhow::Result<IdleFixture> {
    let client_signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        client_signer.address(),
        TOKEN,
        U256::from(10_000_000u64),
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        |deps| deps.idle_timeout = Some(idle),
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    Ok(IdleFixture {
        target: EndpointAddr::new(server_id).with_ip_addr(server_addr),
        client_signer,
        store,
        blobs,
        metrics,
        server_ep,
        server_task,
        _cache_tmp: cache_tmp,
    })
}

/// A raw `cdn/client/v1` delivery parked at its closing-voucher exchange: every
/// chunk has been read, and the server is blocked in `collect_voucher` awaiting
/// payment — so its `serve_stream` future is genuinely held in the serve loop's
/// `inflight` set, for as long as the test declines to pay.
struct StalledDelivery {
    send: SendStream,
    recv: RecvStream,
    /// Wire bytes read, and therefore what the closing voucher must cover.
    wire_bytes: u64,
}

/// Cumulative channel state used to settle a sequence of raw stalled streams.
#[derive(Clone, Copy, Default)]
struct VoucherTotals {
    nonce: U256,
    wire_bytes: u64,
    amount: U256,
}

/// Drive a real paid delivery up to — but not through — its closing voucher.
///
/// The honest requester (`stream_fetch`) owns its connection internally, so a
/// caller cannot watch the server idle-close it, and it pays the moment a
/// voucher is due. This reimplements the receive loop so the test keeps the
/// connection AND chooses when to pay: it sends the `StreamRequest`, reads the
/// signed `StreamResponse` and every `ChunkData` of the whole blob, and stops
/// there. `expected_wire` bytes is the whole delivery, so with a payload well
/// under one voucher interval the server has exactly one (closing) voucher left
/// to collect and is now parked reading it — the stall is a protocol-level
/// rendezvous, not a race, and it holds until the test pays (well inside the
/// handler's 10s `VOUCHER_READ_TIMEOUT`).
async fn stall_delivery_at_closing_voucher(
    conn: &Connection,
    hash: [u8; 32],
    expected_wire: u64,
) -> anyhow::Result<StalledDelivery> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = StreamRequest {
        hash,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    let payload =
        encode_stream_request(&req, None).map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write request: {e}"))?;

    match read_client_msg(&mut recv).await? {
        ClientMessage::StreamResponse(resp) => {
            anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp.error);
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }

    let mut wire_bytes: u64 = 0;
    while wire_bytes < expected_wire {
        match read_client_msg(&mut recv).await? {
            ClientMessage::ChunkData(chunk) => {
                wire_bytes = wire_bytes.saturating_add(chunk.bytes().len() as u64);
            }
            other => anyhow::bail!("expected ChunkData mid-delivery, got {other:?}"),
        }
    }
    anyhow::ensure!(
        wire_bytes == expected_wire,
        "read {wire_bytes} wire bytes, expected exactly {expected_wire}"
    );

    Ok(StalledDelivery {
        send,
        recv,
        wire_bytes,
    })
}

impl StalledDelivery {
    /// Pay the closing voucher and read the delivery out to its `StreamEnd`,
    /// releasing the server's `serve_stream` future. Returns the instant the
    /// CLIENT saw `StreamEnd` — a lower bound on the server-side stream close the
    /// idle clock is required to count from — plus the new cumulative totals.
    async fn pay_and_finish(
        mut self,
        signer: &PrivateKeySigner,
        prior: VoucherTotals,
    ) -> anyhow::Result<(Instant, VoucherTotals)> {
        let amount_delta = min_payment(self.wire_bytes, RATE_PER_MB);
        let settled = VoucherTotals {
            nonce: prior
                .nonce
                .checked_add(U256::ONE)
                .ok_or_else(|| anyhow::anyhow!("voucher nonce overflow"))?,
            wire_bytes: prior
                .wire_bytes
                .checked_add(self.wire_bytes)
                .ok_or_else(|| anyhow::anyhow!("voucher wire-byte total overflow"))?,
            amount: prior
                .amount
                .checked_add(amount_delta)
                .ok_or_else(|| anyhow::anyhow!("voucher amount overflow"))?,
        };
        let voucher = Voucher {
            channel_id: channel_id(),
            // Exactly the advertised-rate minimum for the bytes served, which
            // clears the handler's per-delta `verify_rate` (1% tolerance). The
            // cumulative rate-floor check is inert here: the fixture builds the
            // handler with `delivery_floor = 0`.
            amount: settled.amount,
            nonce: settled.nonce,
            bytes_delivered: U256::from(settled.wire_bytes),
            token: TOKEN,
        }
        .sign(signer, &payment_domain())
        .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
        write_client_msg(
            &mut self.send,
            &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)),
        )
        .await?;

        match read_client_msg(&mut self.recv).await? {
            ClientMessage::VoucherAck => {}
            other => anyhow::bail!("expected VoucherAck, got {other:?}"),
        }
        match read_client_msg(&mut self.recv).await? {
            ClientMessage::StreamEnd => {}
            other => anyhow::bail!("expected StreamEnd, got {other:?}"),
        }
        Ok((Instant::now(), settled))
    }
}

/// Assert `err` is the handler's graceful app-layer idle close: `APP_ERR_NO_ERROR`
/// (0x00) with reason `"idle"`, not a fault code or a transport reset.
fn ensure_graceful_idle_close(err: &ConnectionError) -> anyhow::Result<()> {
    match err {
        ConnectionError::ApplicationClosed(ac) => {
            anyhow::ensure!(
                ac.error_code.into_inner() == 0,
                "idle close must use the no-error code, got {}",
                ac.error_code.into_inner()
            );
            anyhow::ensure!(
                ac.reason.as_ref() == b"idle",
                "idle close reason: {:?}",
                ac.reason
            );
            Ok(())
        }
        other => anyhow::bail!("expected a graceful application idle-close, got {other:?}"),
    }
}

/// ADR 005 §Connection lifetime (#1261): a stalled paid delivery survives the
/// idle window and then settles at the exact byte count. The sibling test above
/// already pins the `inflight.is_empty()` gate itself (it kills the same
/// delete/invert mutations); what is new here is the PARK POINT and the
/// settlement assertions — the server sits past the whole blob in
/// `collect_voucher`, with every byte on the wire and payment pending, so this
/// is the test that fails if the deferral is ever narrowed to the request-read
/// phase alone (e.g. by moving the reaper inside `serve_stream`).
#[tokio::test(flavor = "multi_thread")]
async fn active_delivery_stream_defers_idle_close() -> anyhow::Result<()> {
    // 64 KiB: far under the fixture's 1 MiB voucher interval (hardcoded in
    // `build_handler_full_with_sink`), so the delivery has exactly one (closing)
    // voucher and the park point is unambiguous.
    let payload = vec![0x3Du8; 64 * 1024];
    let idle = Duration::from_millis(150);
    let fx = idle_fixture(&payload, idle).await?;
    let blob = fx.blob(0)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let stalled =
        stall_delivery_at_closing_voucher(&conn, *blob.hash.as_bytes(), blob.wire_bytes).await?;

    // The reaper is disabled while the stream is in flight: no close, however many
    // idle windows pass. `idle * 5` is well past the window yet far short of the
    // 10s voucher-read timeout, so a pass is the gate holding, not the park expiring.
    let premature = tokio::time::timeout(idle * 5, conn.closed()).await;
    anyhow::ensure!(
        premature.is_err(),
        "connection was idle-closed while a paid delivery was in flight: {premature:?}"
    );

    // Delivery completes on resume, and the channel advanced by exactly the bytes
    // that were already on the wire during the stall — proof the stall did not
    // corrupt or truncate the delivery it was holding open.
    let (_completed_at, _totals) = stalled
        .pay_and_finish(&fx.client_signer, VoucherTotals::default())
        .await?;
    let persisted = fx.store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_nonce() == U256::ONE,
        "nonce: {}",
        only.last_nonce()
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(blob.wire_bytes),
        "bytes_delivered: {} (expected {})",
        only.last_bytes_delivered(),
        blob.wire_bytes
    );

    drop(conn);
    client_ep.close().await;
    fx.server_ep.close().await;
    fx.server_task.await?;
    Ok(())
}

/// ADR 005 §Connection lifetime (#1261): the idle clock counts from the LAST
/// STREAM'S CLOSE, not from connection accept. The serve loop builds a fresh
/// `tokio::time::sleep(idle_timeout)` on every iteration; hoisting it into a
/// single `tokio::pin!`'d binding before the loop — a plausible "avoid
/// re-allocating a timer" refactor — would silently make the countdown run from
/// accept, and every other idle test would stay green.
///
/// This one holds a real delivery in flight for `idle * 3`, so a timer armed at
/// connection start has long since expired by the time the stream closes. The
/// close must then arrive a further ~`idle` AFTER completion. Under the hoisted
/// variant the already-expired timer is merely gated off by `is_empty()`, so it
/// fires on the first iteration after `inflight` drains — landing within a
/// round-trip of completion, an order of magnitude under the floor asserted
/// below.
#[tokio::test(flavor = "multi_thread")]
async fn idle_clock_re_arms_from_last_stream_close() -> anyhow::Result<()> {
    let payload = vec![0x4Eu8; 64 * 1024];
    let idle = Duration::from_millis(400);
    let fx = idle_fixture(&payload, idle).await?;
    let blob = fx.blob(0)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let stalled =
        stall_delivery_at_closing_voucher(&conn, *blob.hash.as_bytes(), blob.wire_bytes).await?;

    // Park past `connect + idle` so the two candidate origins are unambiguously
    // separated: a clock counting from accept is already due when we pay.
    tokio::time::sleep(idle * 3).await;
    let (completed_at, _totals) = stalled
        .pay_and_finish(&fx.client_signer, VoucherTotals::default())
        .await?;

    let err = tokio::time::timeout(Duration::from_secs(10), conn.closed())
        .await
        .map_err(|_| {
            anyhow::anyhow!("connection was not idle-closed after the stream completed")
        })?;
    // Both endpoints of this measurement are stamped client-side, so the two
    // loopback flight times largely cancel. What does NOT cancel is that the
    // server writes `StreamEnd` BEFORE `serve_stream` returns, so it re-arms
    // strictly after the instant `completed_at` records: the interval is
    // `idle + (re-arm - StreamEnd write)`, i.e. it can only over-report the
    // server's own delay. That direction is what makes the floor below safe.
    let since_completion = completed_at.elapsed();
    ensure_graceful_idle_close(&err)?;

    // The ORDERING property, with slack for scheduling jitter on a loaded runner:
    // the close is a fresh window measured from the stream's close, not the
    // already-elapsed remainder of a window armed at connection start (which would
    // land within a round-trip of completion, an order of magnitude under this floor).
    let floor = idle.mul_f64(0.8);
    anyhow::ensure!(
        since_completion >= floor,
        "idle clock did not re-arm from the stream's close: closed {since_completion:?} after \
         completion, expected at least {floor:?} (a hoisted timer counts from connection accept)"
    );

    anyhow::ensure!(
        metric_line_present(&fx.metrics.encode()?, "decdn_client_idle_close_total 1"),
        "the re-armed reap must bump decdn_client_idle_close_total"
    );

    client_ep.close().await;
    fx.server_ep.close().await;
    fx.server_task.await?;
    Ok(())
}

/// ADR 005 §Connection lifetime (#1287): every completed stream must re-arm
/// the idle clock, not only the first one. Three paid deliveries finish on the
/// same connection, with each successor starting before the previous fresh
/// idle window expires. The connection survives every cadence, then one final
/// uninterrupted idle window reaps it exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn idle_clock_re_arms_after_each_completed_stream() -> anyhow::Result<()> {
    let payload = vec![0x6Au8; 64 * 1024];
    let idle = Duration::from_secs(1);
    let activity_cadence = idle.mul_f64(0.6);
    let fx = idle_fixture(&payload, idle).await?;
    let blob = fx.blob(0)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let mut totals = VoucherTotals::default();
    let mut final_completion = Instant::now();

    for round in 1..=3 {
        let stalled =
            stall_delivery_at_closing_voucher(&conn, *blob.hash.as_bytes(), blob.wire_bytes)
                .await?;
        let (completed_at, settled) = stalled.pay_and_finish(&fx.client_signer, totals).await?;
        totals = settled;
        final_completion = completed_at;

        if round < 3 {
            let premature = tokio::time::timeout(activity_cadence, conn.closed()).await;
            anyhow::ensure!(
                premature.is_err(),
                "connection closed after re-arm round {round}, before the next activity: \
                 {premature:?}"
            );
        }
    }

    let err = tokio::time::timeout(Duration::from_secs(10), conn.closed())
        .await
        .map_err(|_| anyhow::anyhow!("connection was not reaped after the final idle window"))?;
    // The server re-arms only after writing StreamEnd, so measuring from the
    // client's read can only over-report the server-side idle delay.
    let since_completion = final_completion.elapsed();
    ensure_graceful_idle_close(&err)?;
    let floor = idle.mul_f64(0.8);
    anyhow::ensure!(
        since_completion >= floor,
        "final idle window was not freshly armed: closed {since_completion:?} after completion, \
         expected at least {floor:?}"
    );

    let persisted = fx.store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_nonce() == U256::from(3u64),
        "nonce: {}",
        only.last_nonce()
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(totals.wire_bytes),
        "bytes_delivered: {} (expected {})",
        only.last_bytes_delivered(),
        totals.wire_bytes
    );
    anyhow::ensure!(
        metric_line_present(&fx.metrics.encode()?, "decdn_client_idle_close_total 1"),
        "repeated re-arms must end in exactly one metered idle-close"
    );

    client_ep.close().await;
    fx.server_ep.close().await;
    fx.server_task.await?;
    Ok(())
}

/// ADR 005 §Connection lifetime (#1287): draining one completed future from
/// a connection with two concurrent paid streams must not arm the idle reaper.
/// Only after BOTH streams finish may the fresh idle window begin.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_streams_all_finish_before_idle_clock_arms() -> anyhow::Result<()> {
    let payload_a = vec![0x71u8; 48 * 1024];
    let payload_b = vec![0x82u8; 64 * 1024];
    let idle = Duration::from_millis(300);
    let fx = idle_fixture_with_two_blobs(&payload_a, &payload_b, idle).await?;
    let blob_a = fx.blob(0)?;
    let blob_b = fx.blob(1)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let stalled_a =
        stall_delivery_at_closing_voucher(&conn, *blob_a.hash.as_bytes(), blob_a.wire_bytes)
            .await?;
    let stalled_b =
        stall_delivery_at_closing_voucher(&conn, *blob_b.hash.as_bytes(), blob_b.wire_bytes)
            .await?;

    let (_first_completed_at, totals) = stalled_a
        .pay_and_finish(&fx.client_signer, VoucherTotals::default())
        .await?;

    // One completed future may be drained, but the second delivery remains in
    // `inflight`; surviving three whole windows distinguishes `is_empty()` from
    // a broken one-entry/last-completed interpretation.
    let premature = tokio::time::timeout(idle * 3, conn.closed()).await;
    anyhow::ensure!(
        premature.is_err(),
        "connection was idle-closed while the second stream was still in flight: {premature:?}"
    );

    let (second_completed_at, _totals) =
        stalled_b.pay_and_finish(&fx.client_signer, totals).await?;
    let err = tokio::time::timeout(Duration::from_secs(10), conn.closed())
        .await
        .map_err(|_| {
            anyhow::anyhow!("connection was not reaped after both concurrent streams completed")
        })?;
    // The server re-arms only after writing StreamEnd, so measuring from the
    // client's read can only over-report the server-side idle delay.
    let since_completion = second_completed_at.elapsed();
    ensure_graceful_idle_close(&err)?;
    let floor = idle.mul_f64(0.8);
    anyhow::ensure!(
        since_completion >= floor,
        "idle window started before the second stream closed: reaped {since_completion:?} after \
         completion, expected at least {floor:?}"
    );

    let expected_wire_bytes = blob_a
        .wire_bytes
        .checked_add(blob_b.wire_bytes)
        .ok_or_else(|| anyhow::anyhow!("expected wire-byte total overflow"))?;
    let persisted = fx.store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_nonce() == U256::from(2u64),
        "persisted nonce: {}, expected 2",
        only.last_nonce()
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(expected_wire_bytes),
        "persisted bytes: {}, expected {expected_wire_bytes}",
        only.last_bytes_delivered(),
    );
    anyhow::ensure!(
        metric_line_present(&fx.metrics.encode()?, "decdn_client_idle_close_total 1"),
        "the post-concurrency idle reap must be metered exactly once"
    );

    client_ep.close().await;
    fx.server_ep.close().await;
    fx.server_task.await?;
    Ok(())
}

/// Regression for #1054: a 0-byte blob delivers end-to-end over `cdn/client/v1`.
/// The serve emits no `ChunkData` and no voucher (0 wire bytes), the requester
/// proves the empty stream against the empty root `Hash::new(&[])` and returns
/// empty bytes, and the channel does NOT advance (nonce 0, 0 bytes delivered).
/// Before the fix, `align_range(0, 0, 0)` rejected the whole-empty serve/receive
/// with `RangeOutOfBounds`.
#[tokio::test(flavor = "multi_thread")]
async fn client_delivers_empty_blob() -> anyhow::Result<()> {
    // Empty payload → `hash` is the empty root `blake3::hash(&[])` by construction.
    let payload: Vec<u8> = Vec::new();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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

    anyhow::ensure!(got.as_ref().is_empty(), "empty blob delivers empty bytes");

    // No wire bytes → no voucher → the channel stays at its registered state.
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_nonce() == U256::ZERO,
        "no voucher for a 0-byte blob, nonce: {}",
        only.last_nonce()
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::ZERO,
        "0 bytes delivered, got {}",
        only.last_bytes_delivered()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Watermark-on-error contract (#852) on the `stream_fetch_tracked` path: when a
/// pull errors mid-stream *after* at least one voucher was acked, `progress` must
/// still hold the last acked watermark so the caller can persist what it paid —
/// it must NOT reset to `None`.
///
/// Induced deterministically by deposit exhaustion (no mock server): a 1.5 MiB
/// blob needs two vouchers — cumulative amount 10 then 15 at `RATE_PER_MB` — but
/// the channel deposit is 12. The node acks voucher 1 (amount 10 <= 12) and
/// rejects voucher 2 (amount 15 > 12) as over-deposit, so the fetch errors after
/// one acked voucher. `progress.acked()` must then report voucher 1 (nonce 1),
/// proving the copy-back in `stream_fetch_tracked` runs on the error path.
#[tokio::test(flavor = "multi_thread")]
async fn tracked_watermark_survives_post_ack_error() -> anyhow::Result<()> {
    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB → two vouchers (amount 10 then 15).
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    // Between voucher 1's cumulative amount (10) and voucher 2's (15): voucher 1
    // is acked, voucher 2 is rejected as over-deposit.
    let deposit = U256::from(12u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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
    let mut progress = VoucherProgress::default();

    let result = stream_fetch_tracked(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        decdn_protocol::client::NO_NAMESPACE,
        0,
        0x00c0_ffee,
        PullDeadlines::whole_transfer(Duration::from_secs(20)),
        0,
        0,
        &mut progress,
    )
    .await;

    // Assert the *intended* failure mode, not just any error: voucher 2 must be
    // rejected mid-stream as over-deposit. A regression that errors for some other
    // reason (e.g. a transport fault) should fail this test loudly.
    let err = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("fetch must error when voucher 2 is over-deposit"))?;
    let rejected = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .ok_or_else(|| anyhow::anyhow!("expected UpstreamVoucherRejected, got: {err:?}"))?;
    anyhow::ensure!(
        rejected.reason == VoucherRejectReason::InsufficientDeposit,
        "voucher 2 must be rejected for InsufficientDeposit; got {:?}",
        rejected.reason
    );
    // The contract: the watermark survives the error and reflects the one acked
    // voucher (nonce 1), so the caller can still persist what it paid.
    let acked = progress.acked().ok_or_else(|| {
        anyhow::anyhow!("acked watermark must survive a post-ack error, got None")
    })?;
    anyhow::ensure!(
        acked.0 == U256::from(1u64),
        "exactly one voucher should be acked before the rejection; acked nonce = {}",
        acked.0
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Wiring guard for `decdn node channels` (#749 review, crit 7): a real
/// signed-voucher accept through the live `ClientHandler` must advance the
/// shared in-memory [`VoucherActivity`] clock the admin surface reports.
///
/// The `touch` call site (`handlers/client.rs`) has no other test caller — a
/// dropped or mis-placed stamp would silently leave `seconds_since` at `None`
/// ("never") forever. Here we attach a clock to the handler, drive one
/// successful delivery (which accepts vouchers), and assert the channel now
/// reports `Some(age)`. An untouched channel reports `None`, so `Some` proves
/// the accept path stamped through the attached `Arc`.
#[tokio::test(flavor = "multi_thread")]
async fn accepted_voucher_advances_shared_activity_clock() -> anyhow::Result<()> {
    use decdn_incentive::VoucherActivity;

    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB → crosses a voucher interval
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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
    // Share one `Arc<VoucherActivity>` with the handler — the same wiring
    // `runtime::run` performs (one Arc cloned into the handler and the admin
    // surface). Before any accept the channel is unknown to the clock.
    let activity = Arc::new(VoucherActivity::new());
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        |deps| deps.voucher_activity = Some(Arc::clone(&activity)),
    )?;
    assert_eq!(
        activity.seconds_since(channel_id()),
        None,
        "no voucher accepted yet → clock must report None"
    );

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

    // The accept path stamped the channel through the shared Arc: the admin
    // surface reading the SAME Arc would now report an age rather than "never".
    assert!(
        activity.seconds_since(channel_id()).is_some(),
        "an accepted voucher must advance the shared VoucherActivity clock"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Wiring guard for per-region accounting (#750): a real signed-voucher accept
/// through the live `ClientHandler` must record the served bytes against the
/// resolving region. The `record_served` call site (`handlers/client.rs`) has
/// no other test caller — a dropped or mis-placed call would silently leave
/// every region at zero. We attach an accountant whose stub resolver maps the
/// *client's* node id to "DE", drive one successful delivery, and assert "DE"
/// now carries the delivered bytes as `bytes_out`.
#[tokio::test(flavor = "multi_thread")]
async fn accepted_voucher_records_served_bytes_by_region() -> anyhow::Result<()> {
    /// Stub resolver: client node id -> fixed region.
    struct OneRegion {
        node_id: [u8; 32],
        region: String,
    }
    #[async_trait]
    impl RegionResolver for OneRegion {
        async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
            (node_id == &self.node_id).then(|| self.region.clone())
        }
    }

    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB -> crosses a voucher interval
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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

    // The client endpoint's key is what the handler sees as `client_node_id`.
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let accountant = Arc::new(RegionAccountant::new(Arc::new(OneRegion {
        node_id: *client_id.as_bytes(),
        region: "DE".to_string(),
    })));
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        |deps| deps.region_accountant = Some(Arc::clone(&accountant)),
    )?;
    assert!(
        accountant.snapshot().is_empty(),
        "no delivery yet -> no region buckets"
    );

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
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

    let snap = accountant.snapshot();
    let de = snap
        .iter()
        .find(|r| r.region == "DE")
        .ok_or_else(|| anyhow::anyhow!("expected a DE bucket, got {snap:?}"))?;
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        de.bytes_out == wire,
        "DE bytes_out = {}, expected {wire}",
        de.bytes_out
    );
    anyhow::ensure!(de.bytes_in == 0, "bytes_in must stay 0 (no pull path)");
    anyhow::ensure!(
        snap.iter().all(|r| r.region != UNKNOWN_REGION),
        "resolved client must not fall through to UNKNOWN"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Issue #248: every accepted voucher appends one download receipt with the
/// served blob's hash, the per-interval byte count, the paying client's iroh
/// node id, and the voucher nonce. A 1.5 MiB blob crosses one interval boundary
/// plus a closing voucher, so exactly two receipts are recorded (nonces 1, 2),
/// their `size`s sum to the payload length, and every receipt names the same
/// hash and client node id.
#[tokio::test(flavor = "multi_thread")]
async fn voucher_acceptance_appends_download_receipt() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 1_572_864]; // 1.5 MiB — two vouchers at 1 MiB interval.
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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
    let receipts = Arc::new(VecReceiptLog::default());
    let handler = build_handler_full_with_receipts(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        Arc::clone(&receipts) as Arc<dyn decdn_node::receipt_log::ReceiptLog>,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
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

    let recorded = receipts.snapshot();
    anyhow::ensure!(
        recorded.len() == 2,
        "expected 2 receipts (interval + closing voucher), got {}: {recorded:?}",
        recorded.len()
    );

    let want_hash = hex_lower(hash.as_bytes());
    let want_node = hex_lower(client_node_id.as_bytes());
    let total: u64 = recorded.iter().map(DownloadReceipt::size).sum();
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        total == wire,
        "receipt sizes sum to {total}, expected {wire}"
    );
    for (i, r) in recorded.iter().enumerate() {
        anyhow::ensure!(
            r.hash() == want_hash,
            "receipt[{i}] hash {} != {want_hash}",
            r.hash()
        );
        anyhow::ensure!(
            r.client_node_id() == want_node,
            "receipt[{i}] client_node_id {} != {want_node}",
            r.client_node_id()
        );
        anyhow::ensure!(r.size() > 0, "receipt[{i}] size must be > 0");
        anyhow::ensure!(r.timestamp_secs() > 0, "receipt[{i}] timestamp must be set");
    }
    let nonces: Vec<&str> = recorded
        .iter()
        .map(DownloadReceipt::voucher_nonce)
        .collect();
    anyhow::ensure!(
        nonces == vec!["1", "2"],
        "voucher nonces {nonces:?} != [1, 2]"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Issue #803: receipt-log I/O must never back-pressure paid delivery. Wired
/// through the REAL `ChannelReceiptSink` + background writer (not the inline
/// `DirectReceiptSink`), with the underlying log stalled on every `append`, the
/// full blob must still deliver and hash-verify — the buggy pre-#803 code
/// awaited the append inline before `VoucherAck`, so it would hang here. After
/// releasing the stall and draining the writer, every receipt is recovered,
/// proving the decoupling loses nothing on a clean shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn delivery_completes_while_receipt_writer_is_stalled() -> anyhow::Result<()> {
    let payload = vec![0x3Cu8; 1_572_864]; // 1.5 MiB — two voucher appends.
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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

    // The production sink: a bounded channel feeding a background writer whose
    // log blocks on every append until we release it.
    let blocking_log = Arc::new(BlockingReceiptLog::default());
    let writer_shutdown = CancellationToken::new();
    let (receipt_sink, writer_handle) = spawn_receipt_writer(
        Arc::clone(&blocking_log) as Arc<dyn decdn_node::receipt_log::ReceiptLog>,
        Arc::clone(&metrics),
        writer_shutdown.clone(),
    );
    let handler = build_handler_full_with_sink(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        receipt_sink,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(Arc::clone(&client_signer), deposit);

    // Delivery must complete while every receipt `append` is stalled. The outer
    // timeout is the real assertion: the pre-#803 inline-await would hang.
    let got = tokio::time::timeout(
        Duration::from_secs(20),
        stream_fetch(
            &client_ep,
            target,
            &ctx,
            &slash_domain(),
            server_eth.address(),
            *hash.as_bytes(),
            0,
            0x00c0_ffee,
            Duration::from_secs(20),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("delivery hung while the receipt writer was stalled (#803)"))??;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "delivered bytes mismatch"
    );

    // Release the stall and drain the writer; the audit receipts are not lost.
    blocking_log.release();
    writer_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), writer_handle)
        .await
        .map_err(|_| anyhow::anyhow!("receipt writer did not drain after release"))??;
    let recorded = blocking_log.snapshot();
    let total: u64 = recorded.iter().map(DownloadReceipt::size).sum();
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        total == wire,
        "drained receipt sizes sum to {total}, expected {wire} ({} receipts)",
        recorded.len()
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// Issue #248: a download-receipt write failure is non-fatal. The payment
/// already committed to the channel store, so even with a receipt log that
/// errors on every append, the full blob is delivered and hash-verifies and the
/// channel state still advances. A receipt-log failure must never fail delivery
/// or block payment.
#[tokio::test(flavor = "multi_thread")]
async fn receipt_log_write_failure_does_not_fail_delivery() -> anyhow::Result<()> {
    let payload = vec![0x7Eu8; 1_572_864]; // 1.5 MiB — exercises two voucher appends.
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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
    let handler = build_handler_full_with_receipts(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        Arc::new(FailingReceiptLog) as Arc<dyn decdn_node::receipt_log::ReceiptLog>,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
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
        "delivery must succeed despite receipt-log failure"
    );

    // Payment still committed: the channel advanced to the full byte count.
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(wire),
        "channel state must still advance: bytes_delivered={} (expected {wire})",
        only.last_bytes_delivered()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
        s1.last_nonce() == U256::from(1u64),
        "nonce after 1: {}",
        s1.last_nonce()
    );
    anyhow::ensure!(s1.last_bytes_delivered() == U256::from(payload.len()));

    // Stream 2: resume from the channel's advanced state.
    let ctx2 = ChannelContext {
        prior_nonce: s1.last_nonce(),
        prior_bytes_delivered: s1.last_bytes_delivered(),
        prior_amount: s1.last_amount(),
        client_binding: None,
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
        s2.last_nonce() == U256::from(2u64),
        "nonce after 2: {}",
        s2.last_nonce()
    );
    anyhow::ensure!(
        s2.last_bytes_delivered() == U256::from(2 * payload.len()),
        "cumulative bytes: {}",
        s2.last_bytes_delivered()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Resume with a NON-EMPTY proof spine (#1060): a ~200 KiB blob spans many 16 KiB
/// chunk groups, so the offset resume exercises the production
/// `export_bao_range` ↔ `decode_verified_range` pair over real proof PARENT
/// nodes — not the degenerate single-leaf tree `client_byte_offset_returns_suffix`
/// covers. The non-group-aligned offset (70 KiB) also verifies the decoder trims
/// the leading bytes of the widened [64 KiB, 200 KiB) fetch back to the request.
#[tokio::test(flavor = "multi_thread")]
async fn client_byte_offset_returns_suffix_multi_group() -> anyhow::Result<()> {
    let payload: Vec<u8> = (0..204_800u32).map(|i| (i % 251) as u8).collect();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
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

    // 70 KiB: past the first four 16 KiB groups and NOT group-aligned, so the
    // serve widens down to the 64 KiB boundary and the decoder trims 6 KiB.
    let offset = 70 * 1024u64;
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
        "multi-group offset fetch must return the trimmed suffix"
    );
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    // ADR 038: metered quantity is the bao WIRE size of the widened, aligned range
    // — here strictly larger than the content suffix, because the proof spine
    // carries interior parent nodes (the single-leaf sibling has none).
    let wire = support::bao_wire_len(payload.len() as u64, offset, 0);
    let content_suffix = payload.len() as u64 - offset;
    anyhow::ensure!(
        wire > content_suffix,
        "a multi-group range must carry proof parents (wire {wire} > content {content_suffix})"
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(wire),
        "bytes_delivered should be the aligned bao wire size {wire}, got {}",
        only.last_bytes_delivered()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
    // ADR 038: metered quantity is bao wire bytes (serve aligns up to the 16 KiB group)
    let wire = support::bao_wire_len(payload.len() as u64, offset, 0);
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(wire),
        "bytes_delivered should be the aligned bao wire size {wire}, got {}",
        only.last_bytes_delivered()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
    server_task.await?;
    Ok(())
}

/// #327 boundary + #848 free-egress: a stream request for a channel the node has
/// never persisted is refused *pre-serve* — the node signs `ok: false` with the
/// delivery-side `NotFound` code and ships zero bytes. Previously it served up to
/// one voucher interval (or the whole blob, if smaller) for free and only
/// rejected the voucher mid-stream with `VoucherRejected { WrongChannel }`. The
/// 1.5 MiB blob (larger than the voucher interval) proves the gate fires
/// independent of blob size — not just for sub-interval blobs. Asserting on the
/// server's `StreamResponse` (rather than the buyer's error string) proves the
/// success path was never entered: an `ok: true` would have streamed bytes.
#[tokio::test(flavor = "multi_thread")]
async fn client_unknown_channel_is_rejected() -> anyhow::Result<()> {
    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB — would cross a voucher interval if served
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // Empty store: the channel is unknown to the node.
    let store: Arc<dyn ChannelStateStore> = Arc::new(MemoryChannelStateStore::new());
    let (target, _server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x5678,
    };
    // No binding, no channel: the pure free-egress case (#848). Drive the raw
    // path so we read the server's first reply directly.
    match raw_request(&client_ep, target, &req, None).await? {
        ClientMessage::StreamResponse(resp) => {
            anyhow::ensure!(
                !resp.body.ok,
                "unknown channel must be refused pre-serve, not served"
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
        other => anyhow::bail!("expected a pre-serve StreamResponse refusal, got {other:?}"),
    }

    // The wire `NotFound` is deliberately ambiguous, so the server-side
    // reason counter is the only place this is distinguishable from a cache
    // miss or owner mismatch (#876).
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_unknown_channel_total 1"
        ),
        "unknown-channel refusal must bump its reason counter"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Wire-bounds gate on the client's proposed voucher cadence (ADR 003 §Voucher
/// Interval Negotiation): `voucher_interval_mb` outside `1..=1024` is a protocol
/// error, so the node resets the stream instead of clamping the value into range
/// and serving. Both endpoints of the illegal set are covered — `0` (which a
/// clamp would silently lift to 1 MB) and `MAX + 1` (which a clamp would silently
/// drop to the node's configured cadence), the two cases indistinguishable from
/// an honest proposal once normalized. Driven raw because `stream_fetch` always
/// proposes `None`.
#[tokio::test(flavor = "multi_thread")]
async fn client_out_of_range_voucher_interval_is_refused() -> anyhow::Result<()> {
    let payload = b"an out-of-range cadence never reaches the serve path".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, _owner, _deposit) = seeded_store()?;
    let (target, _server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00ca_de00,
    };

    for interval in [0, decdn_protocol::MAX_VOUCHER_INTERVAL_MB + 1] {
        let ext = StreamRequestExt {
            voucher_interval_mb: Some(interval),
            ..Default::default()
        };
        let res = raw_request(&client_ep, target.clone(), &req, Some(&ext)).await;
        anyhow::ensure!(
            res.is_err(),
            "voucher_interval_mb {interval} is out of range and must reset the stream, \
             not be clamped into range; got {res:?}"
        );
    }

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// A `ChannelStateStore` that hydrates its seeded channels (so vouchers reach
/// the apply path) but fails every `record` with a transient I/O error —
/// exercises the `ChannelError::Store` → `RetryLater` in-band rejection.
#[derive(Debug)]
struct FailingRecordStore {
    inner: MemoryChannelStateStore,
}

impl ChannelStateStore for FailingRecordStore {
    fn load_all(&self) -> Result<Vec<ChannelState>, decdn_incentive::StoreError> {
        self.inner.load_all()
    }

    fn record(&self, _state: &ChannelState) -> Result<(), decdn_incentive::StoreError> {
        Err(decdn_incentive::StoreError::Io(std::io::Error::other(
            "injected transient store failure",
        )))
    }

    fn forget(
        &self,
        channel_id: decdn_incentive::ChannelId,
    ) -> Result<(), decdn_incentive::StoreError> {
        self.inner.forget(channel_id)
    }

    fn get(
        &self,
        channel_id: decdn_incentive::ChannelId,
    ) -> Result<Option<ChannelState>, decdn_incentive::StoreError> {
        self.inner.get(channel_id)
    }
}

/// A transient persist-write failure (`ChannelError::Store`) is surfaced in-band
/// as `VoucherRejected { RetryLater }` and the stream finishes cleanly (no QUIC
/// reset) — the client reads the reason and can resend the same voucher rather
/// than seeing an opaque drop (ADR 003 §332). The node must not `VoucherAck`.
#[tokio::test(flavor = "multi_thread")]
async fn client_transient_store_failure_is_retry_later() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let inner = MemoryChannelStateStore::new();
    inner.record(&ChannelState::new(
        channel_id(),
        client_signer.address(),
        client_signer.address(),
        TOKEN,
        deposit,
    ))?;
    let store: Arc<dyn ChannelStateStore> = Arc::new(FailingRecordStore { inner });

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
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
        *hash.as_bytes(),
        0,
        0x9abc,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("transient store failure must reject the voucher"))?;
    anyhow::ensure!(
        err.to_string().contains("RetryLater"),
        "error should surface the RetryLater rejection: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// A channel past its on-chain `expiresAt` is refused in-band with
/// `VoucherRejected { Expired }` and the stream finishes cleanly (no QUIC reset)
/// — the client reads an actionable reason instead of an opaque drop (#751).
#[tokio::test(flavor = "multi_thread")]
async fn client_expired_channel_is_rejected_with_expired() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    // Seed a channel whose on-chain expiry is already in the past (Unix second
    // `1`), so the serve-gate refuses the first voucher.
    let mut expired = ChannelState::new(
        channel_id(),
        client_signer.address(),
        client_signer.address(),
        TOKEN,
        deposit,
    );
    expired.expires_at = 1;
    store.record(&expired)?;
    let store_dyn: Arc<dyn ChannelStateStore> = store;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
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
        *hash.as_bytes(),
        0,
        0x9abd,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("expired channel must reject the voucher"))?;
    anyhow::ensure!(
        err.to_string().contains("Expired"),
        "error should surface the Expired rejection: {err}"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
    let (target, server_eth, server_ep, server_task, _metrics) =
        spawn_handler_server_with_metrics(cache, store, rate, max_blob, max_streams).await?;
    Ok((target, server_eth, server_ep, server_task))
}

/// As `spawn_handler_server`, but also returns the server's `Arc<Metrics>` so a
/// test can assert a reject counter advanced (#876). The reject path signs a
/// deliberately lossy wire error, so the counter is the only server-side place
/// the precise reason is observable.
async fn spawn_handler_server_with_metrics(
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
    Arc<Metrics>,
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
    Ok((target, server_eth, server_ep, server_task, metrics))
}

/// True if `encoded` (`OpenMetrics` text from `Metrics::encode`) contains `line`
/// as a full line — mirrors the `has_metric_line` helper in `metrics.rs` so the
/// reject counters are asserted on an exact `name value` match, not a substring.
fn metric_line_present(encoded: &str, line: &str) -> bool {
    encoded.lines().any(|l| l == line)
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
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 4096, 16).await?;

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
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_blob_too_large_total 1"
        ),
        "oversized-blob refusal must bump its reason counter (#876)"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Buyer-side size gate (#840): the inverse of `client_blob_too_large_is_refused`.
/// The server has no ceiling and is willing to serve an 8 KiB blob, but the
/// *buyer* passes its own `max_blob_size_bytes`. The buyer must reject the
/// server's oversized `total_bytes` claim before entering the receive loop, so
/// no bytes are buffered or paid (see `fetch_inner`'s ceiling gate for why
/// `StreamResponse::validate()` alone is insufficient).
#[tokio::test(flavor = "multi_thread")]
async fn buyer_rejects_oversized_total_bytes() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 8192];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    // Server ceiling 0 (unlimited) — it would happily serve all 8192 bytes.
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let mut progress = VoucherProgress::default();
    // Buyer ceiling 4096 < 8192 promised → reject before buffering.
    let err = stream_fetch_tracked(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        decdn_protocol::client::NO_NAMESPACE,
        0,
        0x00c1,
        PullDeadlines::whole_transfer(Duration::from_secs(10)),
        4096,
        0,
        &mut progress,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("oversized total_bytes must be refused by the buyer"))?;
    anyhow::ensure!(
        err.to_string().contains("BlobTooLarge"),
        "error should surface the buyer-side BlobTooLarge ceiling: {err}"
    );
    // Rejected before the receive loop: no voucher was ever acked/paid.
    anyhow::ensure!(
        progress.acked().is_none(),
        "no voucher should be paid when the buyer rejects up front: {:?}",
        progress.acked()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Buyer-side RATE gate (#1375): the rate analogue of `buyer_rejects_oversized_total_bytes`.
/// The server quotes `RATE_PER_MB` in a valid, signed `ok == true` `StreamResponse`, but the
/// buyer's ceiling is below it. The buyer must refuse BEFORE paying a single voucher, and the
/// abort must carry the server's OWN signed quote out as `RateAboveCeiling` evidence — that is
/// the rate-manipulation attestation a challenger replays to `SlashJudge` with no re-signing.
#[tokio::test(flavor = "multi_thread")]
async fn buyer_rejects_over_ceiling_rate() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    // Server quotes RATE_PER_MB (10); no size ceiling.
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let mut progress = VoucherProgress::default();
    // Buyer ceiling below the quoted rate → refuse before the first paid interval.
    let buyer_ceiling = RATE_PER_MB - 1;
    let err = stream_fetch_tracked(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        decdn_protocol::client::NO_NAMESPACE,
        0,
        0x00c4,
        PullDeadlines::whole_transfer(Duration::from_secs(10)),
        0,
        buyer_ceiling,
        &mut progress,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("an over-ceiling rate must be refused by the buyer"))?;

    let refused = err
        .downcast_ref::<RateAboveCeiling>()
        .ok_or_else(|| anyhow::anyhow!("must surface a typed RateAboveCeiling, got: {err:#}"))?;
    anyhow::ensure!(
        refused.quoted_rate_per_mb() == RATE_PER_MB
            && refused.ceiling_rate_per_mb() == buyer_ceiling,
        "the error must record the quoted rate {} and the ceiling {}, got {} / {}",
        RATE_PER_MB,
        buyer_ceiling,
        refused.quoted_rate_per_mb(),
        refused.ceiling_rate_per_mb(),
    );
    // The retained evidence is the server's own signed quote and still recovers to it, so a
    // challenger can replay it to `SlashJudge` unchanged.
    let evidence = refused.evidence();
    anyhow::ensure!(
        evidence.body.rate_per_mb == RATE_PER_MB && evidence.body.ok,
        "the retained evidence must be the signed ok:true over-quote"
    );
    // The whole value proposition of retaining the response: its `slash_sig` still
    // ecrecovers to the delivering operator, so it is replayable with no re-signing.
    // (Mirrors `an_open_stage_refusal_preserves_the_signed_stream_response`.)
    let recovered = alloy::primitives::Signature::try_from(evidence.slash_sig.as_slice())?;
    StreamSlashData::from_response_body(&evidence.body).verify_signer(
        &recovered,
        server_eth.address(),
        &slash_domain(),
    )?;
    anyhow::ensure!(
        progress.acked().is_none(),
        "no voucher may be paid when the buyer refuses the rate up front: {:?}",
        progress.acked()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Boundary of the buyer-side gate (#840): the ceiling is inclusive. A blob
/// whose `total_bytes` exactly equals `max_blob_size_bytes` must be accepted
/// (the gate is `total_bytes > ceiling`, strict) — guards against a `>` → `>=`
/// regression that would silently reject every exactly-ceiling-sized blob.
#[tokio::test(flavor = "multi_thread")]
async fn buyer_accepts_blob_at_exact_ceiling() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 8192];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let mut progress = VoucherProgress::default();
    // Buyer ceiling == promised size (8192) → accepted, full blob delivered.
    let got = stream_fetch_tracked(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        decdn_protocol::client::NO_NAMESPACE,
        0,
        0x00c2,
        PullDeadlines::whole_transfer(Duration::from_secs(10)),
        8192,
        0,
        &mut progress,
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "exact-ceiling blob must deliver intact"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Boundary of the buyer-side RATE gate (#1375): inclusive, like the blob-size
/// gate above. A quote EXACTLY equal to the buyer ceiling must be accepted and
/// deliver in full (the gate is `rate_per_mb > ceiling`, strict) — guards against
/// a `>` → `>=` regression that would reject an honest node quoting right at the
/// buyer's ceiling. The sibling of `buyer_accepts_blob_at_exact_ceiling`.
#[tokio::test(flavor = "multi_thread")]
async fn buyer_accepts_rate_at_exact_ceiling() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    // Server quotes RATE_PER_MB; buyer ceiling set to EXACTLY RATE_PER_MB.
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let mut progress = VoucherProgress::default();
    let got = stream_fetch_tracked(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        decdn_protocol::client::NO_NAMESPACE,
        0,
        0x00c5,
        PullDeadlines::whole_transfer(Duration::from_secs(10)),
        0,
        RATE_PER_MB, // ceiling == quote → accepted
        &mut progress,
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "a quote exactly at the buyer ceiling must deliver intact"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Delivery-progress hook (#1118): `stream_fetch_tracked_with_progress` invokes
/// the callback as bytes arrive with `(wire_bytes_received, wire_bytes_expected)`.
/// The expected total is fixed once the signed `StreamResponse` arrives, received
/// bytes advance monotonically, and the final observation reaches the full
/// promised wire length — the contract `decdn fetch`'s progress bar relies on.
#[tokio::test(flavor = "multi_thread")]
async fn progress_callback_reports_monotonic_delivery() -> anyhow::Result<()> {
    // Multi-chunk payload (256 KiB > CHUNK_SIZE) so the callback fires repeatedly
    // and the monotonic-advance assertion has intermediate points to check.
    let payload = vec![0xABu8; 256 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let mut progress = VoucherProgress::default();
    // `ProgressCallback` is `'static`, so the closure owns an `Arc` handle to the
    // shared sink rather than borrowing a stack local.
    let observations = Arc::new(std::sync::Mutex::new(Vec::<(u64, u64)>::new()));
    let sink = Arc::clone(&observations);
    let record = move |received: u64, expected: u64| {
        if let Ok(mut v) = sink.lock() {
            v.push((received, expected));
        }
    };
    let got = stream_fetch_tracked_with_progress(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        decdn_protocol::client::NO_NAMESPACE,
        0,
        0x00c3,
        PullDeadlines::whole_transfer(Duration::from_secs(10)),
        0, // unlimited buyer blob-size ceiling
        0, // unlimited buyer rate ceiling
        &mut progress,
        Some(&record),
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "progress-tracked fetch must still deliver the blob intact"
    );

    // Clone the observations out and drop the guard before the cleanup awaits
    // below (clippy `await_holding_lock`).
    let obs: Vec<(u64, u64)> = observations
        .lock()
        .map_err(|_| anyhow::anyhow!("progress mutex poisoned"))?
        .clone();
    anyhow::ensure!(!obs.is_empty(), "progress callback must fire at least once");
    let expected = obs.last().map_or(0, |&(_, e)| e);
    anyhow::ensure!(expected > 0, "expected wire length must be positive");
    anyhow::ensure!(
        obs.iter().all(|&(_, e)| e == expected),
        "expected total must stay constant across the pull"
    );
    let mut prev = 0u64;
    for &(received, _) in &obs {
        anyhow::ensure!(
            received >= prev,
            "progress must be monotonically non-decreasing ({received} < {prev})"
        );
        anyhow::ensure!(
            received <= expected,
            "progress ({received}) must never exceed the expected total ({expected})"
        );
        prev = received;
    }
    anyhow::ensure!(
        prev == expected,
        "final progress ({prev}) must reach the expected wire length ({expected})"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Build a [`DeniedHashes`] from store hashes, the way config resolution would.
fn denied_hashes(hashes: &[Hash]) -> decdn_cache::DeniedHashes {
    decdn_cache::DeniedHashes::new(
        hashes
            .iter()
            .map(|h| decdn_cache::LeafHash::from_bytes(*h.as_bytes()))
            .collect(),
    )
}

fn content_with_origins(
    origins: &[alloy::primitives::Address],
) -> decdn_common::config::ResolvedContent {
    decdn_common::config::ResolvedContent {
        denied_origins: origins.iter().copied().collect(),
        ..Default::default()
    }
}

/// [`spawn_handler_server_with_metrics`] with an ADR 011 origin deny-set wired in.
async fn spawn_handler_server_with_deny(
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    deny: Arc<decdn_node::content_deny::ContentDenylist>,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<PrivateKeySigner>,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<Metrics>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        0,
        16,
        |deps| deps.content_deny = deny,
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth, server_ep, server_task, metrics))
}

/// ADR 011 §Local Denylist / §On Blacklist Event step 2: a denied hash is
/// refused even though the node still physically holds the bytes.
///
/// Two properties in one test. `CacheEngine::refuses` makes the blob report
/// absent everywhere (defence in depth — this is what also suppresses the probe
/// `has_blob` signature and DHT republish), while the serve gate above the
/// availability check is what turns that into `HashBlacklisted` with its own
/// metric rather than an indistinguishable `NotFound`.
///
/// Without a test at this level the whole gate could be deleted and every other
/// test in the suite would stay green — which is exactly what happened.
#[tokio::test(flavor = "multi_thread")]
async fn denylisted_hash_is_refused_even_when_held() -> anyhow::Result<()> {
    let payload = b"content under a local takedown order".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    anyhow::ensure!(cache.has(hash).await?, "held before the takedown");

    // Deliberately NOT evicted — the bytes stay on disk. Denial alone must be
    // enough to stop serving.
    cache.set_denied(&denied_hashes(&[hash]));
    anyhow::ensure!(
        !cache.has(hash).await?,
        "a denied blob must report absent, exactly as an evicted one does — \
         defence in depth behind the serve gate"
    );

    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

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
        0x00e2,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a denylisted blob must be refused even when held"))?;
    anyhow::ensure!(
        err.to_string().contains("HashBlacklisted") || err.to_string().contains("refused"),
        "error should surface HashBlacklisted: {err}"
    );
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_hash_denied_total 1"
        ),
        "the takedown refusal must bump its own reason counter, not the eviction one"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// ADR 011 §`StreamRequest` Response: a GOVERNANCE takedown must answer the
/// same wire code as the operator's own denylist entry above.
///
/// The failure this pins shipped once already. Governance entries reached the
/// serve path only as cache evictions, so they answered `EvictedSinceProbe`
/// while local entries answered `HashBlacklisted` — one request told a client
/// which list a hash was on, and since the governance list is public on-chain,
/// that made `HashBlacklisted` a unique fingerprint for "this operator denied it
/// privately". Precisely the map of an operator's legal exposure the ADR
/// forecloses.
///
/// The metric split is the part that MAY differ, and does: it is the operator's
/// own gauge and no client can read it.
#[tokio::test(flavor = "multi_thread")]
async fn governance_denied_hash_is_refused_as_hash_blacklisted() -> anyhow::Result<()> {
    let payload = b"content under a governance takedown".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // The watcher denies *and* evicts; deny only, so the test proves the deny
    // gate is what produced the code rather than the eviction arm below it.
    cache.set_chain_denied_one(hash, true);
    anyhow::ensure!(!cache.is_denied(hash), "not on the LOCAL list");

    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

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
        0x00e4,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a governance-blacklisted blob must be refused"))?;
    anyhow::ensure!(
        err.to_string().contains("HashBlacklisted") || err.to_string().contains("refused"),
        "a governance takedown must sign HashBlacklisted, NOT EvictedSinceProbe: {err}"
    );
    let encoded = metrics.encode()?;
    anyhow::ensure!(
        metric_line_present(
            &encoded,
            "decdn_serve_stream_rejected_chain_hash_denied_total 1"
        ),
        "the governance refusal has its own operator-side counter"
    );
    anyhow::ensure!(
        metric_line_present(&encoded, "decdn_serve_stream_rejected_hash_denied_total 0"),
        "...and must not be miscounted as a local denylist hit"
    );
    anyhow::ensure!(
        metric_line_present(
            &encoded,
            "decdn_serve_stream_rejected_evicted_since_probe_total 0"
        ),
        "...nor land on the eviction counter, which is now non-takedown evictions only"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// ADR 011 §On Blacklist Event: "In-flight streams for a blacklisted hash are
/// terminated at the next MB boundary."
///
/// The open-time gates cannot cover this: a takedown that lands *after* a stream
/// opens would otherwise let a multi-GB blob run to completion minutes into a
/// one-hour removal order, and serving past the compliance window is slashable
/// (ADR 026 §Slashing and burn).
///
/// The takedown is landed only once a voucher has been accepted, which is what
/// makes this test exercise the mid-stream path rather than racing the open-time
/// gate: a voucher proves the request was already admitted and bytes are
/// flowing. The blob is sized to many voucher intervals so boundaries remain
/// after the flip.
#[tokio::test(flavor = "multi_thread")]
async fn takedown_mid_stream_terminates_the_delivery() -> anyhow::Result<()> {
    // 16 intervals at the harness's 1 MB `voucher_interval_mb`.
    let payload = vec![0x5Au8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let deny_cache = cache.clone();
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, Arc::clone(&store), RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let fetch_ep = client_ep.clone();
    let server_addr = server_eth.address();
    let hash_bytes = *hash.as_bytes();
    let fetch = tokio::spawn(async move {
        stream_fetch(
            &fetch_ep,
            target,
            &ctx,
            &slash_domain(),
            server_addr,
            hash_bytes,
            0,
            0x00e5,
            Duration::from_secs(30),
        )
        .await
    });

    // Wait for the first accepted voucher — the store is written durably on
    // every acceptance, so a non-zero `last_amount` means delivery is under way.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !store
        .load_all()?
        .iter()
        .any(|state| state.last_amount() > U256::ZERO)
    {
        anyhow::ensure!(
            Instant::now() < deadline,
            "no voucher was ever accepted; the test never reached the mid-stream path"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    deny_cache.set_chain_denied_one(hash, true);

    let err = fetch
        .await?
        .err()
        .ok_or_else(|| anyhow::anyhow!("delivery must not complete through a takedown"))?;
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_terminated_takedown_total 1"
        ),
        "the cut-off must be metered as an in-flight termination, not a completed \
         delivery ({err})"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// ADR 011 §On Blacklist Event. A channel funded by a blacklisted origin is
/// refused with `OriginBlacklisted` — including on a CACHE MISS, which is the
/// path that previously fell through to a plain `NotFound` because every miss
/// arm returns before the gate's old position.
///
/// `NotFound` is the one answer that must never be given here: it tells the
/// client to retry elsewhere and pay again, when every node will refuse it.
#[tokio::test(flavor = "multi_thread")]
async fn blacklisted_funder_is_refused_on_a_cache_miss() -> anyhow::Result<()> {
    let cache_dir = tempfile::tempdir()?;
    let cache = CacheEngine::open(cache_dir.path(), vec![], 16).await?;
    let (store, signer, deposit) = seeded_store()?;
    let funder = signer.address();
    let deny = Arc::new(decdn_node::content_deny::ContentDenylist::new(
        &content_with_origins(&[funder]),
    ));

    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_deny(cache, store, deny).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(signer, deposit);
    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        [0x5a; 32], // never cached — the miss path
        0,
        0x00e3,
        Duration::from_secs(10),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a blacklisted funder must be refused"))?;
    anyhow::ensure!(
        err.to_string().contains("OriginBlacklisted") || err.to_string().contains("refused"),
        "error should surface OriginBlacklisted, not NotFound: {err}"
    );
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_origin_denied_total 1"
        ),
        "the compliance gauge must count a miss-path refusal too"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// A blob evicted between probe and stream request is refused with
/// `EvictedSinceProbe` (distinct from never-had-it `NotFound`).
#[tokio::test(flavor = "multi_thread")]
async fn client_evicted_since_probe_is_refused() -> anyhow::Result<()> {
    let payload = b"evicted between probe and stream".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    cache.evict(hash).await?; // logically gone: has() now false, is_evicted() true.
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

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
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_evicted_since_probe_total 1"
        ),
        "evicted-since-probe refusal must bump its reason counter (#876)"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00ba_d001,
    };
    let res = raw_request(&client_ep, target, &req, Some(&ext)).await;
    anyhow::ensure!(
        res.is_err(),
        "a binding recovering a different address must reset the stream, got {res:?}"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
    let (target, _server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

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
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
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

    // Wire-indistinguishable from a cache miss or unknown channel (all
    // `NotFound`), so only the reason counter proves the owner-mismatch arm
    // ran (#876).
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_owner_mismatch_total 1"
        ),
        "owner-mismatch refusal must bump its reason counter"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// What [`delegate_signer_store`] hands back: the seeded store, the funder
/// signer, and the delegate voucher signer.
type DelegateSignerFixture = (
    Arc<dyn ChannelStateStore>,
    Arc<PrivateKeySigner>,
    Arc<PrivateKeySigner>,
);

/// A channel store seeded with one channel whose FUNDER and pinned
/// `voucher_signer` are distinct addresses — the publisher-pays delegate shape,
/// where the funder put up the deposit and a throwaway hot key signs vouchers.
/// Returns the store, the funder signer, and the delegate signer.
fn delegate_signer_store() -> anyhow::Result<DelegateSignerFixture> {
    let funder = Arc::new(PrivateKeySigner::random());
    let delegate = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id(),
        funder.address(),
        delegate.address(),
        TOKEN,
        deposit,
    ))?;
    Ok((store, funder, delegate))
}

/// Build the `StreamRequestExt` carrying a valid client binding for `signer`
/// over `client_node_id` — the honest requester never sends one, so the binding
/// tests construct it by hand.
fn binding_ext(
    signer: &PrivateKeySigner,
    client_node_id: B256,
) -> anyhow::Result<StreamRequestExt> {
    Ok(StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: signer.address().into(),
            binding_signature: sign_binding_for(signer, client_node_id)?,
        }),
        ..Default::default()
    })
}

/// The binding gate is a *signer* question: a connection bound as the channel's
/// pinned `voucher_signer` is authorized, even though that address never funded
/// the channel. Its vouchers are the ones the channel will accept, so it is the
/// only identity that can pay for this delivery.
#[tokio::test(flavor = "multi_thread")]
async fn binding_matching_the_delegate_signer_is_authorized() -> anyhow::Result<()> {
    let payload = b"delegate-signed channel is served".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, _funder, delegate) = delegate_signer_store()?;
    let (target, _server_eth, server_ep, server_task, _metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let ext = binding_ext(&delegate, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e01,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        ClientMessage::StreamResponse(resp) => {
            anyhow::ensure!(
                resp.body.ok,
                "the pinned voucher signer must be authorized, got {:?}",
                resp.error
            );
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// The mirror of the above: a connection bound as the *funder* of a delegated
/// channel is refused with `OwnerMismatch` (wire `NotFound`). The funder holds
/// no voucher authority on this channel, so its vouchers would fail
/// `WrongSigner` mid-stream after free bytes had already shipped.
#[tokio::test(flavor = "multi_thread")]
async fn binding_matching_only_the_funder_is_refused() -> anyhow::Result<()> {
    let payload = b"the funder cannot spend a delegated channel".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, funder, _delegate) = delegate_signer_store()?;
    let (target, _server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let ext = binding_ext(&funder, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e02,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        ClientMessage::StreamResponse(resp) => {
            anyhow::ensure!(!resp.body.ok, "expected ok:false for the funder binding");
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
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_owner_mismatch_total 1"
        ),
        "the refusal must be the owner-mismatch arm, not some other NotFound"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// COMPLIANCE REGRESSION GUARD (ADR 011 §On Blacklist Event). The takedown gate
/// keys on the channel's FUNDER, never on its voucher signer. A blacklisted
/// funder that delegates signing to a clean throwaway key must still be refused
/// with `OriginBlacklisted`.
///
/// This test fails the moment any blacklist / `content_deny` check is re-keyed
/// onto `voucher_signer` — which is exactly the silent compliance break a
/// blanket `state.client` → `voucher_signer` rename would cause.
#[tokio::test(flavor = "multi_thread")]
async fn blacklisted_funder_is_refused_even_behind_a_clean_delegate() -> anyhow::Result<()> {
    let payload = b"a clean delegate does not launder a blacklisted funder".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, funder, delegate) = delegate_signer_store()?;
    // Only the funder is on the deny-set; the delegate signer is clean.
    let deny = Arc::new(decdn_node::content_deny::ContentDenylist::new(
        &content_with_origins(&[funder.address()]),
    ));
    let (target, _server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_deny(cache, store, deny).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let ext = binding_ext(&delegate, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e03,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        ClientMessage::StreamResponse(resp) => {
            anyhow::ensure!(
                !resp.body.ok,
                "a blacklisted funder must be refused behind a clean delegate"
            );
            anyhow::ensure!(
                matches!(
                    resp.error,
                    Some(decdn_protocol::client::StreamError::OriginBlacklisted)
                ),
                "expected OriginBlacklisted, got {:?}",
                resp.error
            );
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_origin_denied_total 1"
        ),
        "the compliance gauge must count the delegated-channel refusal"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// The full delegated round trip: a channel whose `voucher_signer` is NOT the
/// funder is served end to end on delegate-signed vouchers, and the persisted
/// state keeps the two addresses apart.
///
/// Every other delegated-channel test stops at the open-time gate's verdict.
/// This one crosses the whole seam — request admitted, delegate-signed vouchers
/// accepted at each interval, bytes assembled and hash-verified — so a
/// regression that accepted the open but rejected the delegate's vouchers
/// mid-stream cannot hide.
#[tokio::test(flavor = "multi_thread")]
async fn delegate_signed_vouchers_carry_a_delivery_to_completion() -> anyhow::Result<()> {
    // 1.5 MiB crosses one voucher-interval boundary plus a closing voucher, so
    // at least two delegate-signed vouchers are verified.
    let payload = vec![0xC3u8; 1_572_864];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, funder, delegate) = delegate_signer_store()?;
    let (target, server_eth, server_ep, server_task, _metrics) =
        spawn_handler_server_with_metrics(cache, Arc::clone(&store), RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    // The requester signs with the DELEGATE — the funder's key never appears.
    let ctx = channel_context(Arc::clone(&delegate), U256::from(10_000_000u64));
    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x00de_1e05,
        Duration::from_secs(30),
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "delegate-signed delivery returned {} bytes, expected {}",
        got.len(),
        payload.len()
    );

    let states = store.load_all()?;
    let state = states
        .first()
        .ok_or_else(|| anyhow::anyhow!("the channel must still be persisted"))?;
    anyhow::ensure!(
        state.last_nonce() >= U256::from(2u64),
        "expected at least two accepted delegate-signed vouchers, got nonce {}",
        state.last_nonce()
    );
    // ADR 038: the metered quantity is bao wire bytes, not payload bytes.
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        state.last_bytes_delivered() == U256::from(wire),
        "the persisted byte count must cover the whole blob: {} (expected {wire})",
        state.last_bytes_delivered()
    );
    anyhow::ensure!(
        state.client == funder.address() && state.voucher_signer == delegate.address(),
        "funder and signer must stay distinct across a completed delivery"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// COMPLIANCE REGRESSION GUARD (ADR 011 §On Blacklist Event), mid-stream half.
///
/// The per-MB in-flight takedown re-checks in `window.rs` / `delivery.rs` are
/// keyed on the FUNDER. Only a comment stops them being re-keyed onto
/// `voucher_signer`, and the open-time delegated-funder test passes either way
/// because it never reaches a voucher boundary. This one blacklists the funder
/// AFTER delivery is under way on a channel whose signer is a clean, never-
/// blacklisted delegate: if the re-check moved to the signer, the stream would
/// run to completion and this test fails.
#[tokio::test(flavor = "multi_thread")]
async fn blacklisting_the_funder_mid_stream_cuts_off_a_delegated_delivery() -> anyhow::Result<()> {
    // 16 intervals at the harness's 1 MB `voucher_interval_mb`, so plenty of
    // boundaries remain after the deny-set flip.
    let payload = vec![0x6Bu8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, funder, delegate) = delegate_signer_store()?;
    // Starts empty — nothing is denied at open time, so the request is admitted
    // and the cut-off can only come from the mid-stream re-check.
    let deny = Arc::new(decdn_node::content_deny::ContentDenylist::empty());
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_deny(cache, Arc::clone(&store), Arc::clone(&deny)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(Arc::clone(&delegate), U256::from(10_000_000u64));
    let fetch_ep = client_ep.clone();
    let server_addr = server_eth.address();
    let hash_bytes = *hash.as_bytes();
    let fetch = tokio::spawn(async move {
        stream_fetch(
            &fetch_ep,
            target,
            &ctx,
            &slash_domain(),
            server_addr,
            hash_bytes,
            0,
            0x00de_1e06,
            Duration::from_secs(30),
        )
        .await
    });

    // Wait for the first accepted (delegate-signed) voucher: proof the request
    // was admitted and bytes are flowing, so the flip lands mid-stream rather
    // than racing the open-time gate.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !store
        .load_all()?
        .iter()
        .any(|state| state.last_amount() > U256::ZERO)
    {
        anyhow::ensure!(
            Instant::now() < deadline,
            "no delegate-signed voucher was ever accepted; the test never reached the \
             mid-stream path"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Only the FUNDER goes on the deny-set; the delegate signer stays clean.
    deny.apply_chain_origin(funder.address(), true);

    let err = fetch.await?.err().ok_or_else(|| {
        anyhow::anyhow!(
            "delivery must not complete once the funder is blacklisted — the mid-stream \
             re-check has been re-keyed onto the voucher signer"
        )
    })?;
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_terminated_takedown_total 1"
        ),
        "the cut-off must be metered as an in-flight termination, not a completed \
         delivery ({err})"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Open a cache pre-seeded with TWO distinct blobs (via a shared filesystem
/// origin, dropped after population), mirroring [`cache_with_blob`] for the
/// concurrent-pull test that requests two different hashes at once.
async fn cache_with_two_blobs(
    a: &[u8],
    b: &[u8],
) -> anyhow::Result<(
    CacheEngine,
    decdn_cache::Hash,
    decdn_cache::Hash,
    tempfile::TempDir,
)> {
    let hash_a = decdn_cache::Hash::new(a);
    let hash_b = decdn_cache::Hash::new(b);
    let origin_dir = tempfile::tempdir()?;
    for (payload, hash) in [(a, hash_a), (b, hash_b)] {
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let dir = origin_dir.path().join(shard);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(hex.as_str()), payload)?;
    }

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
    let cache = CacheEngine::open(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;
    let _ = cache.get(hash_a).await?; // populate local store
    let _ = cache.get(hash_b).await?;
    drop(origin_dir);
    Ok((cache, hash_a, hash_b, cache_dir))
}

/// Concurrent pulls on ONE channel coordinate through a shared [`ChannelLedger`]:
/// two simultaneous fetches of two distinct blobs issue vouchers in strict nonce
/// order (the ledger serializes the sign→send→ack→commit cycle), so BOTH succeed
/// and the channel advances monotonically. This is the fix for the collision the
/// old `client_concurrent_same_channel_accepts_one_voucher` test pinned, where two
/// un-coordinated streams both signed voucher nonce 1 and only one was accepted.
#[tokio::test(flavor = "multi_thread")]
async fn client_concurrent_same_channel_both_succeed() -> anyhow::Result<()> {
    let payload_a = vec![0x5Au8; 4096];
    let payload_b = vec![0xA5u8; 8192];
    let (cache, hash_a, hash_b, _cache_tmp) = cache_with_two_blobs(&payload_a, &payload_b).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, Arc::clone(&store), RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    // ONE channel context, ONE shared ledger seeded fresh (all `prior_* == ZERO`),
    // shared across both concurrent pulls via `Arc`.
    let ctx = channel_context(Arc::clone(&signer), deposit);
    let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));
    let server_addr = server_eth.address();
    let sd = slash_domain();
    let (ra, rb) = tokio::join!(
        stream_fetch_shared(
            &client_ep,
            target.clone(),
            &ctx,
            &ledger,
            &sd,
            server_addr,
            *hash_a.as_bytes(),
            0,
            0x00aa,
            PullDeadlines::whole_transfer(Duration::from_secs(15)),
            0,
            0,
        ),
        stream_fetch_shared(
            &client_ep,
            target.clone(),
            &ctx,
            &ledger,
            &sd,
            server_addr,
            *hash_b.as_bytes(),
            0,
            0x00bb,
            PullDeadlines::whole_transfer(Duration::from_secs(15)),
            0,
            0,
        ),
    );
    // BOTH succeed: the shared ledger serialized voucher issuance, so neither
    // collided on a nonce. Each pull returns its own verified blob.
    let bytes_a = ra.map_err(|e| anyhow::anyhow!("pull A failed: {e:?}"))?;
    let bytes_b = rb.map_err(|e| anyhow::anyhow!("pull B failed: {e:?}"))?;
    anyhow::ensure!(bytes_a.as_ref() == payload_a.as_slice(), "blob A mismatch");
    anyhow::ensure!(bytes_b.as_ref() == payload_b.as_slice(), "blob B mismatch");

    // The channel advanced monotonically: at least one voucher per pull (>= 2
    // total), and the cumulative bytes cover BOTH payloads. Both pulls completed, so
    // every voucher they sent optimistically has been acked — `committed` carries the
    // full total.
    let final_cum = ledger.committed();
    anyhow::ensure!(
        final_cum.nonce >= U256::from(2u64),
        "ledger nonce: {} (expected >= 2)",
        final_cum.nonce
    );
    let total_bytes = U256::from(payload_a.len() + payload_b.len());
    anyhow::ensure!(
        final_cum.bytes == total_bytes,
        "ledger bytes: {} (expected {})",
        final_cum.bytes,
        total_bytes
    );

    // The node's persisted watermark matches the ledger: the channel applied every
    // voucher in order, ending at the same cumulative bytes.
    let persisted = store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted channel"))?;
    anyhow::ensure!(
        only.last_bytes_delivered() == total_bytes,
        "persisted bytes_delivered: {} (expected {})",
        only.last_bytes_delivered(),
        total_bytes
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
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
    // Wire-indistinguishable from an unknown channel or owner mismatch (all
    // `NotFound`); the reason counter is the only proof the cache-miss arm ran
    // (#876). No pull-through is configured, so `!filled` is deterministic.
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_cache_miss_total 1"
        ),
        "cache-miss refusal must bump its reason counter"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// **#327 / #527 idempotency guard.** A re-observed `ChannelOpened` (which the
/// settlement watcher *will* replay after any RPC-error resubscription) must
/// NOT reset an already-advanced voucher watermark via `register_open_channel`
/// — doing so would reopen the replay window. Here the channel is already
/// known with `last_nonce = 5` (hydrated from the store at construction); a
/// fresh `register_open_channel` for the same id (nonce 0) must be a no-op.
#[tokio::test]
async fn register_open_channel_is_idempotent_and_preserves_watermark() -> anyhow::Result<()> {
    let (cache, _tmp) = empty_cache().await?;
    let store = Arc::new(MemoryChannelStateStore::new());
    let client = PrivateKeySigner::random().address();

    // Pre-seed an advanced watermark, as if vouchers had been accepted. The
    // `last_*` fields are private (#751), so build the watermark via `hydrate`.
    let advanced = ChannelState::hydrate(
        channel_id(),
        client,
        client,
        TOKEN,
        U256::from(10_000_000u64),
        U256::from(4_321u64),
        U256::from(5u64),
        U256::from(2_048u64),
        Some([0x11; 65]),
        0,
        false,
    );
    store.record(&advanced)?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let server_eth = Arc::new(PrivateKeySigner::random());
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        fresh_key().public(),
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    // A replayed ChannelOpened arrives as a brand-new (nonce 0) state.
    handler
        .register_open_channel(ChannelState::new(
            channel_id(),
            client,
            client,
            TOKEN,
            U256::from(10_000_000u64),
        ))
        .await?;

    let after = store
        .get(channel_id())?
        .ok_or_else(|| anyhow::anyhow!("channel vanished"))?;
    anyhow::ensure!(
        after.last_nonce() == U256::from(5u64),
        "re-observed ChannelOpened reset the watermark to {} (reopened #527 replay window)",
        after.last_nonce()
    );
    anyhow::ensure!(after.last_amount() == U256::from(4_321u64));
    Ok(())
}

/// `update_channel_deposit` (#327 `ChannelToppedUp` handling): raises a tracked
/// deposit, is a no-op for a non-increasing value (deposits only grow; the
/// event is not provider-indexed), and a no-op for an untracked channel.
#[tokio::test]
async fn update_channel_deposit_raises_and_is_idempotent() -> anyhow::Result<()> {
    let (cache, _tmp) = empty_cache().await?;
    let store = Arc::new(MemoryChannelStateStore::new());
    let client = PrivateKeySigner::random().address();
    store.record(&ChannelState::new(
        channel_id(),
        client,
        client,
        TOKEN,
        U256::from(10_000_000u64),
    ))?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let server_eth = Arc::new(PrivateKeySigner::random());
    let store_dyn: Arc<dyn ChannelStateStore> = store.clone();
    let handler = build_handler(
        fresh_key().public(),
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    // Non-increasing => no-op.
    handler
        .update_channel_deposit(channel_id(), U256::from(5_000_000u64))
        .await?;
    let deposit = |s: &Arc<MemoryChannelStateStore>| -> anyhow::Result<U256> {
        Ok(s.get(channel_id())?
            .ok_or_else(|| anyhow::anyhow!("missing"))?
            .deposit)
    };
    anyhow::ensure!(
        deposit(&store)? == U256::from(10_000_000u64),
        "lower deposit ignored"
    );

    // Higher => raised and persisted.
    handler
        .update_channel_deposit(channel_id(), U256::from(20_000_000u64))
        .await?;
    anyhow::ensure!(
        deposit(&store)? == U256::from(20_000_000u64),
        "top-up raised deposit"
    );

    // Untracked channel => no-op, no error, no row created.
    let other = B256::repeat_byte(0x99);
    handler
        .update_channel_deposit(other, U256::from(50_000_000u64))
        .await?;
    anyhow::ensure!(
        store.get(other)?.is_none(),
        "untracked top-up must not create state"
    );
    Ok(())
}

/// Key-rotation runbook gauges are absolute snapshots of the live inbound
/// channel map: hydration, add, top-up, settle, and replay must never drift.
#[tokio::test]
async fn channel_metrics_follow_lifecycle_without_replay_double_count() -> anyhow::Result<()> {
    let (cache, _tmp) = empty_cache().await?;
    let store = Arc::new(MemoryChannelStateStore::new());
    let client = PrivateKeySigner::random().address();
    store.record(&ChannelState::new(
        channel_id(),
        client,
        client,
        TOKEN,
        U256::from(10_000_000u64),
    ))?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let server_eth = Arc::new(PrivateKeySigner::random());
    let store_dyn: Arc<dyn ChannelStateStore> = store;
    let handler = build_handler(
        fresh_key().public(),
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    let assert_snapshot = |open: u64, deposit: u64| -> anyhow::Result<()> {
        let encoded = metrics.encode()?;
        anyhow::ensure!(
            metric_line_present(&encoded, &format!("decdn_channels_open {open}")),
            "open-channel gauge mismatch:\n{encoded}"
        );
        anyhow::ensure!(
            metric_line_present(&encoded, &format!("decdn_channel_deposit_usdc {deposit}")),
            "channel-deposit gauge mismatch:\n{encoded}"
        );
        Ok(())
    };

    assert_snapshot(1, 10_000_000)?;

    // Replaying the hydrated open must not add either the channel or deposit.
    handler
        .register_open_channel(ChannelState::new(
            channel_id(),
            client,
            client,
            TOKEN,
            U256::from(1u64),
        ))
        .await?;
    assert_snapshot(1, 10_000_000)?;

    let second = B256::repeat_byte(0x88);
    handler
        .register_open_channel(ChannelState::new(
            second,
            client,
            client,
            TOKEN,
            U256::from(7_000_000u64),
        ))
        .await?;
    assert_snapshot(2, 17_000_000)?;

    handler
        .update_channel_deposit(second, U256::from(9_000_000u64))
        .await?;
    assert_snapshot(2, 19_000_000)?;
    handler
        .update_channel_deposit(second, U256::from(9_000_000u64))
        .await?;
    assert_snapshot(2, 19_000_000)?;

    handler.forget_channel(channel_id()).await?;
    assert_snapshot(1, 9_000_000)?;
    handler.forget_channel(channel_id()).await?;
    assert_snapshot(1, 9_000_000)?;
    Ok(())
}

// ===========================================================================
// Node-to-node cache-miss pull-through authorization gate (#831)
//
// The miss-hook that triggers a *paid* upstream pull must fire only for a
// request that PROVES ownership of the named channel — channel ids are public
// on-chain, so existence cannot authorize spend. A `CountingOrigin` (returns
// NotFound but counts every fetch) stands in for the paid `NodeOrigin`, so a
// test can distinguish "the gate blocked the pull" (0 fetches) from "the pull
// was attempted" (>=1 fetch) even though both cases return NotFound to the
// client.
// ===========================================================================

/// An origin that counts how many times it was asked and always reports the blob
/// absent. As the last origin in the chain it models the paid network pull
/// without spending: a nonzero count means the miss-hook reached the (paid)
/// pull, which the authorization gate must prevent for unauthorized requests.
#[derive(Debug)]
struct CountingOrigin {
    hits: Arc<std::sync::atomic::AtomicUsize>,
}

impl decdn_cache::Origin for CountingOrigin {
    fn fetch(
        &self,
        _hash: decdn_cache::Hash,
        _max_bytes: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<decdn_cache::origin::OriginFetch, decdn_cache::OriginPullError>,
                > + Send
                + '_,
        >,
    > {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(decdn_cache::origin::OriginFetch::NotFound) })
    }

    fn kind(&self) -> decdn_cache::OriginKind {
        decdn_cache::OriginKind::Peer
    }
}

/// The pull-through gate authorizes ONLY a request proving ownership of the
/// named channel: an unbound request and a validly-bound-but-wrong-owner request
/// must NOT reach the paid pull (the counting origin stays at 0), while the
/// channel owner's bound request does. All three return `NotFound` to the
/// client (the stand-in origin has nothing); the security property is whether
/// the paid pull was attempted at all.
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_gate_authorizes_only_channel_owner() -> anyhow::Result<()> {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cache_tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(
        cache_tmp.path(),
        vec![Arc::new(CountingOrigin {
            hits: Arc::clone(&hits),
        }) as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;

    // Channel owned by `owner`; this is the only identity authorized to pull.
    let (store, owner, _deposit) = seeded_store()?;
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    // Arm pull-through, as the runtime does when the feature is enabled.
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
        |deps| deps.pull_through = Some(std::time::Duration::from_secs(10)),
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // A hash the node does not have → a miss that would trigger the pull.
    let miss_hash = [0xEEu8; 32];
    let req = StreamRequest {
        hash: miss_hash,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0091_1001,
    };

    // 1) Unbound request: no binding → not authorized → pull NOT attempted.
    let (c1, _) = local_endpoint(fresh_key(), vec![]).await?;
    let _ = raw_request(&c1, target.clone(), &req, None).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "an unbound request must NOT trigger a paid pull"
    );
    c1.close().await;

    // 2) Bound to the WRONG owner: valid signature, but not the channel's client
    //    → not authorized → pull NOT attempted.
    let intruder_sk = fresh_key();
    let intruder_node_id = B256::from(*intruder_sk.public().as_bytes());
    let (c2, _) = local_endpoint(intruder_sk, vec![]).await?;
    let intruder = PrivateKeySigner::random();
    let ext_intruder = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: intruder.address().into(),
            binding_signature: sign_binding_for(&intruder, intruder_node_id)?,
        }),
        ..Default::default()
    };
    let _ = raw_request(&c2, target.clone(), &req, Some(&ext_intruder)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "a binding for a non-owner address must NOT trigger a paid pull"
    );
    c2.close().await;

    // 3) Bound to the channel OWNER: authorized → the pull IS attempted (the
    //    counting origin is reached exactly once).
    let owner_sk = fresh_key();
    let owner_node_id = B256::from(*owner_sk.public().as_bytes());
    let (c3, _) = local_endpoint(owner_sk, vec![]).await?;
    let ext_owner = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: owner.address().into(),
            binding_signature: sign_binding_for(&owner, owner_node_id)?,
        }),
        ..Default::default()
    };
    let _ = raw_request(&c3, target.clone(), &req, Some(&ext_owner)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "the channel owner's bound request MUST trigger the pull exactly once, got {}",
        hits.load(std::sync::atomic::Ordering::SeqCst)
    );
    c3.close().await;

    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// The pull-through gate is the same *signer* question as the binding gate: on
/// a channel with a delegated voucher signer, only the delegate can make this
/// node front upstream USDC. The funder — which cannot produce an acceptable
/// voucher — must not.
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_authorizes_the_delegate_not_the_funder() -> anyhow::Result<()> {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cache_tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(
        cache_tmp.path(),
        vec![Arc::new(CountingOrigin {
            hits: Arc::clone(&hits),
        }) as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;

    let (store, funder, delegate) = delegate_signer_store()?;
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
        |deps| deps.pull_through = Some(std::time::Duration::from_secs(10)),
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let req = StreamRequest {
        hash: [0xEDu8; 32], // never cached — a miss that would trigger the pull
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e04,
    };

    // 1) Bound as the FUNDER: it holds no voucher authority here, so it must not
    //    make this node spend upstream.
    let funder_sk = fresh_key();
    let funder_node_id = B256::from(*funder_sk.public().as_bytes());
    let (c1, _) = local_endpoint(funder_sk, vec![]).await?;
    let ext_funder = binding_ext(&funder, funder_node_id)?;
    let _ = raw_request(&c1, target.clone(), &req, Some(&ext_funder)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "the funder of a delegated channel must NOT trigger a paid pull"
    );
    c1.close().await;

    // 2) Bound as the pinned DELEGATE signer: authorized → the pull is attempted.
    let delegate_sk = fresh_key();
    let delegate_node_id = B256::from(*delegate_sk.public().as_bytes());
    let (c2, _) = local_endpoint(delegate_sk, vec![]).await?;
    let ext_delegate = binding_ext(&delegate, delegate_node_id)?;
    let _ = raw_request(&c2, target.clone(), &req, Some(&ext_delegate)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "the pinned voucher signer MUST trigger the pull exactly once, got {}",
        hits.load(std::sync::atomic::Ordering::SeqCst)
    );
    c2.close().await;

    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Build a pull-through-armed handler over a `CountingOrigin` with an ADR 011
/// origin deny-set wired in, and return the dialable target plus the hit
/// counter. Shared by the two blacklist-subject tests below, which differ only
/// in which address is on the deny-set.
async fn spawn_counting_pull_server_with_deny(
    store: Arc<dyn ChannelStateStore>,
    denied: &[alloy::primitives::Address],
) -> anyhow::Result<(
    EndpointAddr,
    Arc<std::sync::atomic::AtomicUsize>,
    Endpoint,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
)> {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cache_tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(
        cache_tmp.path(),
        vec![Arc::new(CountingOrigin {
            hits: Arc::clone(&hits),
        }) as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;
    let deny = Arc::new(decdn_node::content_deny::ContentDenylist::new(
        &content_with_origins(denied),
    ));
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
        |deps| {
            deps.pull_through = Some(std::time::Duration::from_secs(10));
            deps.content_deny = deny;
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, hits, server_ep, server_task, cache_tmp))
}

/// ADR 011 keys compliance on the FUNDER, so a blacklisted funder must not make
/// this node front upstream USDC even when a perfectly clean delegate key is
/// doing the signing. The delegate's binding satisfies the *spend-authority*
/// half of `pull_authorized`; the funder check is the only thing standing
/// between a sanctioned address and this operator's egress bill.
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_refuses_a_blacklisted_funder_behind_a_clean_delegate() -> anyhow::Result<()> {
    let (store, funder, delegate) = delegate_signer_store()?;
    let (target, hits, server_ep, server_task, _cache_tmp) =
        spawn_counting_pull_server_with_deny(store, &[funder.address()]).await?;

    let delegate_sk = fresh_key();
    let delegate_node_id = B256::from(*delegate_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(delegate_sk, vec![]).await?;
    let ext = binding_ext(&delegate, delegate_node_id)?;
    let req = StreamRequest {
        hash: [0xB1u8; 32], // never cached — a miss that would trigger the pull
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00b1_ac11,
    };
    let _ = raw_request(&client_ep, target, &req, Some(&ext)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "a blacklisted funder must NOT trigger a paid pull, clean delegate or not"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// The mirror image, and the deliberate behaviour change: a CLEAN funder whose
/// pinned `voucher_signer` happens to be on the deny-set IS authorized to pull.
/// A takedown sanctions the money, not whichever throwaway key signs the
/// vouchers, so the signer's deny-set membership is not a compliance event here.
///
/// This test fails the moment anyone re-adds an `is_origin_denied` check on the
/// bound (signer) address — the pre-split check that silently became a signer
/// check when the funder/signer roles were split. It also pins the agreement
/// with `dispatch.rs`, whose serve gate is funder-only: without this, the two
/// paths could drift apart unnoticed, since with `voucher_signer == client` on
/// every channel that exists today no other test can tell them apart.
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_allows_a_clean_funder_with_a_blacklisted_delegate() -> anyhow::Result<()> {
    let (store, _funder, delegate) = delegate_signer_store()?;
    let (target, hits, server_ep, server_task, _cache_tmp) =
        spawn_counting_pull_server_with_deny(store, &[delegate.address()]).await?;

    let delegate_sk = fresh_key();
    let delegate_node_id = B256::from(*delegate_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(delegate_sk, vec![]).await?;
    let ext = binding_ext(&delegate, delegate_node_id)?;
    let req = StreamRequest {
        hash: [0xB2u8; 32], // never cached — a miss that would trigger the pull
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00b2_ac11,
    };
    let _ = raw_request(&client_ep, target, &req, Some(&ext)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "a clean funder's delegated request MUST trigger the pull exactly once, got {}",
        hits.load(std::sync::atomic::Ordering::SeqCst)
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

// ===========================================================================
// #859 — handler-level outer-deadline regression. `ClientHandler::try_pull_through`
// wraps the whole pull in ONE `tokio::time::timeout`. Before #859 that outer
// deadline was set EQUAL to the per-candidate budget (`node_pull_timeout_sec`),
// so a pull needing more than one per-candidate budget's wall-clock (e.g.
// candidate #1 stalls a full budget, then #2 delivers) was cancelled before it
// could finish. The derived `outer_pull_deadline` (N × each candidate's three
// sequential stages, + slack) must accommodate it. A single slow origin taking
// longer than one per-candidate
// budget models that scenario through the real handler — the layer the bug
// actually lived in (the NodeOrigin-level fallthrough test cannot, since
// `NodeOrigin::fetch` has no outer wrapper).
// ===========================================================================

/// An origin that returns the blob after a fixed delay — models a pull whose
/// total wall-clock exceeds one per-candidate budget (stall-then-fallback).
#[derive(Debug)]
struct SlowOrigin {
    payload: Vec<u8>,
    delay: Duration,
}

impl decdn_cache::Origin for SlowOrigin {
    fn fetch(
        &self,
        _hash: decdn_cache::Hash,
        _max_bytes: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<decdn_cache::origin::OriginFetch, decdn_cache::OriginPullError>,
                > + Send
                + '_,
        >,
    > {
        let payload = self.payload.clone();
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(decdn_cache::origin::OriginFetch::found_one_shot(
                payload.into(),
            ))
        })
    }

    fn kind(&self) -> decdn_cache::OriginKind {
        decdn_cache::OriginKind::Peer
    }
}

/// Drive one authorized owner request for a hash served only by a `SlowOrigin`
/// taking `origin_delay`, with the handler's outer pull-through deadline set to
/// `outer_deadline`. Returns whether the foreground pull completed and filled the
/// store (probed via a cloned cache handle after the response settles). No
/// background fill is wired, so this isolates the foreground outer-deadline
/// behaviour (a background warm could otherwise fill the store after the fact and
/// mask the broken case).
async fn pull_through_fills_under_deadline(
    outer_deadline: Duration,
    origin_delay: Duration,
) -> anyhow::Result<bool> {
    let payload = vec![0x5Au8; 4096];
    let want = decdn_cache::Hash::new(&payload);
    let cache_tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(
        cache_tmp.path(),
        vec![Arc::new(SlowOrigin {
            payload: payload.clone(),
            delay: origin_delay,
        }) as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;
    let cache_probe = cache.clone();

    let (store, owner, _deposit) = seeded_store()?;
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        &loopback_domains(),
        0,
        16,
        |deps| deps.pull_through = Some(outer_deadline),
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let req = StreamRequest {
        hash: *want.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0091_1001,
    };
    let owner_sk = fresh_key();
    let owner_node_id = B256::from(*owner_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(owner_sk, vec![]).await?;
    let ext_owner = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: owner.address().into(),
            binding_signature: sign_binding_for(&owner, owner_node_id)?,
        }),
        ..Default::default()
    };
    // The handler answers only after `try_pull_through` resolves (populate
    // completes or the outer deadline fires), so the store state is settled by
    // the time this returns.
    let _ = raw_request(&client_ep, target, &req, Some(&ext_owner)).await?;
    client_ep.close().await;

    let filled = cache_probe.has(want).await?;
    server_ep.close().await;
    server_task.await?;
    Ok(filled)
}

/// #859 regression: a slow pull (1.5s) exceeding one 1s per-candidate budget is
/// abandoned by an outer deadline equal to that budget (the pre-#859 wiring), but
/// completes under the derived `outer_pull_deadline`. This is the only test in
/// the suite that fails if the `pull_through` deadline is re-wired to the per-candidate
/// value (re-introducing #859).
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_outer_deadline_accommodates_a_slow_pull() -> anyhow::Result<()> {
    let per = Duration::from_secs(1);
    let stall = Duration::from_secs(1);
    let slow = Duration::from_millis(1500); // > per, well under outer_pull_deadline(per, stall)

    // Pre-#859 wiring: outer == per_candidate cancels the slow pull → store empty.
    anyhow::ensure!(
        !pull_through_fills_under_deadline(per, slow).await?,
        "an outer deadline equal to the per-candidate budget must abandon the slow pull (the #859 bug)"
    );
    // Fixed wiring: the derived outer deadline accommodates it → store filled.
    anyhow::ensure!(
        pull_through_fills_under_deadline(
            decdn_node::selection::outer_pull_deadline(per, stall),
            slow
        )
        .await?,
        "the derived outer deadline must let a pull exceeding one per-candidate budget complete"
    );
    Ok(())
}

/// Open a bidi stream, send one arbitrary [`ClientMessage`], and return the
/// first decoded reply — the cooperative-close analogue of [`raw_request`].
async fn raw_message_request(
    client_ep: &Endpoint,
    target: EndpointAddr,
    msg: &ClientMessage,
) -> anyhow::Result<ClientMessage> {
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))?;
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read frame (stream reset?): {e}"))?;
    let (m, _rest) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    Ok(m)
}

/// End-to-end cooperative close (ADR 003 §Cooperative close): a channel with an
/// advanced watermark answers a `CooperativeCloseRequest` with a waiver that
/// recovers to the node's eth key over the on-chain `CooperativeClose` typed
/// data, persists the no-longer-serving flag, and refuses subsequent delivery.
#[tokio::test(flavor = "multi_thread")]
async fn cooperative_close_signs_waiver_persists_flag_and_stops_serving() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // Seed a channel with a real watermark (nonce 5) — there is something to waive.
    let signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let last_amount = U256::from(4_000u64);
    let last_nonce = U256::from(5u64);
    let last_bytes = U256::from(2_048u64);
    let store_inner = Arc::new(MemoryChannelStateStore::new());
    store_inner.record(&ChannelState::hydrate(
        channel_id(),
        signer.address(),
        signer.address(),
        TOKEN,
        deposit,
        last_amount,
        last_nonce,
        last_bytes,
        Some([0x11; 65]),
        0,
        false,
    ))?;
    let store: Arc<dyn ChannelStateStore> = store_inner.clone();
    let (target, server_eth, server_ep, server_task, _metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;

    // (1) Request the waiver.
    let reply = raw_message_request(
        &client_ep,
        target.clone(),
        &ClientMessage::CooperativeCloseRequest(CooperativeCloseRequest {
            channel_id: channel_id().0,
        }),
    )
    .await?;
    let auth = match reply {
        ClientMessage::CooperativeCloseAuth(a) => a,
        other => anyhow::bail!("expected CooperativeCloseAuth, got {other:?}"),
    };
    anyhow::ensure!(auth.channel_id == channel_id().0, "channel id echoed");
    anyhow::ensure!(
        U256::from_be_bytes(auth.amount) == last_amount,
        "amount = watermark"
    );
    anyhow::ensure!(
        U256::from_be_bytes(auth.nonce) == last_nonce,
        "nonce = watermark"
    );
    anyhow::ensure!(
        U256::from_be_bytes(auth.bytes_delivered) == last_bytes,
        "bytes = watermark"
    );

    // (2) The waiver recovers to the NODE's eth key over the CooperativeClose
    //     typed data — exactly what the on-chain `_verifyCooperativeClose` checks.
    let close = CooperativeClose {
        channel_id: channel_id(),
        amount: last_amount,
        nonce: last_nonce,
        bytes_delivered: last_bytes,
        token: TOKEN,
    };
    let waiver = SignedCooperativeClose {
        close,
        signature: Signature::try_from(auth.signature.as_slice())?,
    };
    waiver.verify_signer(server_eth.address(), &payment_domain())?;

    // (3) The no-longer-serving flag was persisted.
    let persisted = store_inner
        .get(channel_id())?
        .ok_or_else(|| anyhow::anyhow!("channel missing after waiver"))?;
    anyhow::ensure!(
        persisted.cooperative_close_signed(),
        "cooperative-close flag must persist after signing"
    );

    // (4) A subsequent delivery request is refused — the blob is present, so
    //     without the waiver it would serve; the flag makes it `ok: false`.
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id().0,
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 1,
    };
    match raw_request(&client_ep, target, &req, None).await? {
        ClientMessage::StreamResponse(r) => {
            anyhow::ensure!(
                !r.body.ok,
                "delivery must be refused on a cooperatively-closed channel"
            );
        }
        other => anyhow::bail!("expected StreamResponse, got {other:?}"),
    }

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Build a cache whose local store is EMPTY but whose filesystem origin holds
/// `payload`, so a `cdn/client/v1` cache miss must reactively pull the origin
/// through to serve. Both temp dirs are returned so the caller keeps the origin
/// alive across the fetch (unlike [`cache_with_two_blobs`], which pre-populates
/// the store and drops the origin).
///
/// The engine is opened with an `Arc<CacheMetrics>` (returned) so callers can
/// assert `origin_fetches` — the counter that pins whether the node actually
/// read its origin (`1`) or short-circuited before any egress (`0`). `open`
/// wires no metrics, so `origin_fetches.inc()` would be a no-op there.
async fn empty_cache_with_fs_origin(
    payload: &[u8],
) -> anyhow::Result<(
    CacheEngine,
    decdn_cache::Hash,
    Arc<CacheMetrics>,
    tempfile::TempDir,
    tempfile::TempDir,
)> {
    let hash = decdn_cache::Hash::new(payload);
    let origin_dir = tempfile::tempdir()?;
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let dir = origin_dir.path().join(shard);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(hex.as_str()), payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
    let cache_metrics = Arc::new(CacheMetrics::default());
    let cache = CacheEngine::open_full(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cache_metrics)),
        Duration::ZERO,
    )
    .await?;
    // Local store intentionally left unpopulated: `has(hash)` is false until a
    // pull-through fills it, which is what the tests below exercise.
    anyhow::ensure!(!cache.has(hash).await?, "cache store must start empty");
    Ok((cache, hash, cache_metrics, origin_dir, cache_dir))
}

/// Spawn a `ClientHandler` server with buffered origin pull-through attached
/// (`ClientHandlerDeps.pull_through`), returning the dial target and the server's voucher
/// signer address. Unlike [`spawn_handler_server`], the handler is built inline
/// so pull-through can be wired before it is spawned.
async fn spawn_pull_through_server(
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        0,
        16,
        |deps| deps.pull_through = Some(Duration::from_secs(20)),
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth.address(), server_ep, server_task))
}

/// #1115: a direct client fetch that sends the ADR 005 client identity binding
/// authorizes reactive origin pull-through — with the blob present only in the
/// node's filesystem origin (an empty local store), the miss populates from the
/// origin and the bytes are delivered + hash-verified.
#[tokio::test(flavor = "multi_thread")]
async fn bound_client_fetch_triggers_reactive_origin_pull_through() -> anyhow::Result<()> {
    let payload = vec![0x7Bu8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_pull_through_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    // Sign the binding over the CLIENT's own node id with the channel-owning key,
    // under the handler's binding domain — exactly what `pull_authorized` checks.
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
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
    // The blob existed only in the fs origin, so serving it proves the node
    // reactively pulled the origin exactly once (not a double-pull, not some
    // other path).
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 1,
        "expected exactly 1 origin fetch, got {}",
        cache_metrics.origin_fetches.get()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// #1115 control: the SAME setup WITHOUT a client binding is refused. An
/// unauthenticated request fails `pull_authorized`, so the buffered pull-through
/// never runs and the origin-only blob is a clean `CacheMiss` delivery refusal —
/// pinning that the binding is what unlocks the reactive path.
#[tokio::test(flavor = "multi_thread")]
async fn unbound_client_fetch_is_refused_on_origin_only_blob() -> anyhow::Result<()> {
    let payload = vec![0x7Bu8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_pull_through_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    // No `with_client_binding`: `verified_client` stays `None`.
    let ctx = channel_context(Arc::clone(&signer), deposit);

    match stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(_) => anyhow::bail!("unbound fetch of an origin-only blob must be refused"),
        Err(e) => anyhow::ensure!(
            e.to_string().contains("delivery refused"),
            "expected a delivery-refused error, got: {e}"
        ),
    }
    // The anti-griefing guarantee: an unauthorized request must not make the node
    // front upstream/origin work. Prove the gate short-circuited BEFORE any origin
    // egress — not that it read the origin and then refused.
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 0,
        "unbound request must not trigger an origin fetch, got {}",
        cache_metrics.origin_fetches.get()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Like [`spawn_pull_through_server`] but arms ONLY the reactive LOCAL-origin
/// populate (`ClientHandlerDeps.local_populate`) — NOT the node→node buffered/window paths.
/// This is the cache-only-operator wiring (#1116): `[cache.origin]` set,
/// `node_to_node_pull_through_enabled` off.
async fn spawn_local_populate_server(
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        0,
        16,
        |deps| deps.local_populate = Some(Duration::from_secs(20)),
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth.address(), server_ep, server_task))
}

/// A server with BOTH the reactive local populate AND a node→node window origin
/// armed — but the window origin is left UNPROVISIONED (a dead peer path). If the
/// serve path (wrongly) preferred the peer window path over the local origin for
/// a whole-blob request, the pull would hit this dead origin and the fetch would
/// be refused; a successful local serve proves local-first (#1116 shadowing fix).
async fn spawn_local_and_window_server(
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        0,
        16,
        |deps| {
            deps.local_populate = Some(Duration::from_secs(20));
            deps.set_window_pull_through(
                Arc::new(decdn_node::node_origin::NodeOrigin::new()),
                decdn_cache::Bytes::new(64 * 1024),
            );
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth.address(), server_ep, server_task))
}

/// #1116: a node with node→node pull-through DISABLED — only the reactive
/// LOCAL-origin populate armed — still reactively serves a blob present solely in
/// its own filesystem origin to a bound, channel-owning client. This is the
/// cache-only-operator flow that was a silent `NotFound` before decoupling local
/// populate from `node_to_node_pull_through_enabled`.
#[tokio::test(flavor = "multi_thread")]
async fn local_populate_serves_own_origin_with_node_to_node_off() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_local_populate_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
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
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 1,
        "expected exactly 1 local origin fetch, got {}",
        cache_metrics.origin_fetches.get()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// #1116 control: the SAME local-populate-only setup WITHOUT a client binding is
/// refused — reactive local populate is gated on the SAME proven channel
/// ownership as the node→node paths (`pull_authorized`), so an S3 origin's egress
/// isn't fronted for an unauthenticated request. No origin fetch is triggered.
#[tokio::test(flavor = "multi_thread")]
async fn unbound_local_populate_is_refused() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_local_populate_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(Arc::clone(&signer), deposit);

    match stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(_) => anyhow::bail!("unbound local-populate fetch must be refused"),
        Err(e) => anyhow::ensure!(
            e.to_string().contains("delivery refused"),
            "expected a delivery-refused error, got: {e}"
        ),
    }
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 0,
        "unbound request must not trigger a local origin fetch, got {}",
        cache_metrics.origin_fetches.get()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// #1116 (window-shadowing fix): with BOTH the reactive local populate AND a
/// node→node window origin armed, a whole-blob request for a blob the operator
/// holds in its OWN filesystem origin is served from that local origin — the
/// window (peer) path, here deliberately dead/unprovisioned, is never taken.
/// Before the fix a whole-blob miss went straight to the peer window path and
/// never read the local origin.
#[tokio::test(flavor = "multi_thread")]
async fn local_origin_preferred_over_peer_window_path() -> anyhow::Result<()> {
    let payload = vec![0x3Cu8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_local_and_window_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
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
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 1,
        "the local origin must be preferred (served) over the peer window path, got {} fetches",
        cache_metrics.origin_fetches.get()
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// #1116: with node→node OFF and only the local populate armed, a bound fetch for
/// a blob ABSENT from the node's own origin cleanly terminates as a delivery
/// refusal (not a hang or a masked error) after the local origin is consulted
/// exactly once — the miss path (`try_local_populate` returns
/// `FillOutcome::CleanMiss` → falls
/// through to a `CacheMiss`, since node→node is the only further tier and it's
/// off). Guards that the local-first insertion neither shadows a would-be
/// node→node fallthrough nor short-circuits the miss handling.
#[tokio::test(flavor = "multi_thread")]
async fn local_populate_miss_is_clean_cache_miss() -> anyhow::Result<()> {
    let payload = vec![0x2Bu8; 64 * 1024];
    let hash = decdn_cache::Hash::new(&payload);
    // A filesystem origin that does NOT contain the blob (empty dir), plus an
    // empty local store — so the reactive local populate is a genuine miss.
    let origin_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
    let cache_metrics = Arc::new(CacheMetrics::default());
    let cache = CacheEngine::open_full(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cache_metrics)),
        Duration::ZERO,
    )
    .await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task, metrics) =
        spawn_fault_server(cache, Arc::clone(&store), FaultTiers::LocalOnly).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    match stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(_) => anyhow::bail!("a blob absent from the local origin must be refused"),
        // A GENUINE absence must still sign `NotFound` — the #1129 counterpart of
        // the hard-fault tests below. Pinned to the wire error (not just "refused")
        // so a future change that over-eagerly promotes clean misses to
        // `InternalError` fails here.
        Err(e) => anyhow::ensure!(
            e.to_string().contains("delivery refused") && e.to_string().contains("NotFound"),
            "expected a signed NotFound for a genuine absence, got: {e}"
        ),
    }
    // The local origin WAS consulted on the miss (proving the local-first path
    // ran, not that it was skipped), then the request fell through to a clean
    // refusal because node→node is off — the only further tier.
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 1,
        "local origin should be consulted once on the miss, got {}",
        cache_metrics.origin_fetches.get()
    );
    // The inverse of the hard-fault tests, and the guard that makes the pair
    // airtight: a genuine absence must be metered as a cache miss and must NOT
    // trip the internal-error counter. Over-promoting clean misses would report a
    // healthy-but-empty node as broken and steer clients away from it.
    assert_reject_reason(&metrics, 0, 1)?;

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// Which reactive fill tiers a fault-test server arms (#1129).
#[derive(Clone, Copy)]
enum FaultTiers {
    /// Cache-only operator: local populate only, node→node OFF.
    LocalOnly,
    /// Local populate + an UNPROVISIONED node→node window origin (a dead peer
    /// path that cleanly misses) — exercises the fault surviving a fall-through.
    LocalAndWindow,
    /// The buffered node→node tier only (`ClientHandlerDeps.pull_through`), no window origin.
    /// This is the `try_pull_through` path — the one whose `HardFault` no other
    /// test reaches.
    Buffered,
}

/// Spawn a serve handler over `cache` with the given fill tiers armed, returning
/// the server `Metrics` so a test can assert the per-reason reject counter.
///
/// The counter is not a nicety here: the wire error is deliberately lossy (seven
/// distinct reject reasons collapse to the single `NotFound` code), so the
/// per-reason metric is the ONLY server-side place the true cause is observable —
/// which is exactly the signal an operator needs while their origin is down.
async fn spawn_fault_server(
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    tiers: FaultTiers,
) -> anyhow::Result<(
    EndpointAddr,
    Address,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<Metrics>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        0,
        16,
        |deps| match tiers {
            FaultTiers::LocalOnly => deps.local_populate = Some(Duration::from_secs(20)),
            FaultTiers::LocalAndWindow => {
                deps.local_populate = Some(Duration::from_secs(20));
                deps.set_window_pull_through(
                    Arc::new(decdn_node::node_origin::NodeOrigin::new()),
                    decdn_cache::Bytes::new(64 * 1024),
                );
            }
            FaultTiers::Buffered => deps.pull_through = Some(Duration::from_secs(20)),
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((
        target,
        server_eth.address(),
        server_ep,
        server_task,
        metrics,
    ))
}

/// Assert the serve-reject counters recorded exactly one refusal, of `reason`.
/// Pins BOTH directions — the reason fired and its sibling did not — so a change
/// that over-promotes clean misses to `InternalError` (which would steer clients
/// off a healthy node) fails just as loudly as one that under-reports a fault.
fn assert_reject_reason(
    metrics: &Arc<Metrics>,
    internal_error: u64,
    cache_miss: u64,
) -> anyhow::Result<()> {
    let encoded = metrics.encode()?;
    for (name, want) in [
        (
            "decdn_serve_stream_rejected_internal_error_total",
            internal_error,
        ),
        ("decdn_serve_stream_rejected_cache_miss_total", cache_miss),
    ] {
        let line = format!("{name} {want}");
        anyhow::ensure!(
            metric_line_present(&encoded, &line),
            "expected metric line `{line}`; counters were:\n{}",
            encoded
                .lines()
                .filter(|l| l.starts_with("decdn_serve_stream_rejected"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    Ok(())
}

/// An origin that always fails with a TRANSIENT backend error — the shape of an
/// S3 5xx that survives retry exhaustion, an open circuit breaker, or an fs I/O
/// fault. The engine surfaces this as `CacheError::OriginError`, which is NOT an
/// absence: the blob may well exist and be servable once the origin recovers
/// (#1129).
///
/// Distinct from [`CountingOrigin`] in the way that matters: that one is
/// `OriginKind::Peer`, which `populate_local` deliberately SKIPS, so it can never
/// exercise the local tier. This one is `OriginKind::Http` — a local-tier origin
/// the reactive populate does consult.
#[derive(Debug)]
struct FailingOrigin {
    hits: Arc<std::sync::atomic::AtomicUsize>,
}

impl decdn_cache::Origin for FailingOrigin {
    fn fetch(
        &self,
        _hash: decdn_cache::Hash,
        _max_bytes: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<decdn_cache::origin::OriginFetch, decdn_cache::OriginPullError>,
                > + Send
                + '_,
        >,
    > {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {
            Err(decdn_cache::OriginPullError::Transient(anyhow::anyhow!(
                "synthetic origin outage (HTTP 503)"
            )))
        })
    }

    fn kind(&self) -> decdn_cache::OriginKind {
        decdn_cache::OriginKind::Http
    }
}

/// A cache whose sole configured origin is a [`FailingOrigin`] — the operator's
/// own backend is down. The local store starts empty, so a serve request is a
/// miss whose reactive local populate hits a hard fault.
async fn empty_cache_with_failing_origin(
    payload: &[u8],
) -> anyhow::Result<(
    CacheEngine,
    decdn_cache::Hash,
    Arc<std::sync::atomic::AtomicUsize>,
    tempfile::TempDir,
)> {
    let hash = decdn_cache::Hash::new(payload);
    let cache_dir = tempfile::tempdir()?;
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let origin = Arc::new(FailingOrigin {
        hits: Arc::clone(&hits),
    });
    let cache = CacheEngine::open_full(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::new(CacheMetrics::default())),
        Duration::ZERO,
    )
    .await?;
    anyhow::ensure!(!cache.has(hash).await?, "cache store must start empty");
    Ok((cache, hash, hits, cache_dir))
}

/// #1129: a cache-only operator (node→node OFF) whose OWN origin hard-faults must
/// refuse with a retryable `InternalError` — NOT a signed `NotFound`.
///
/// The refusal is EIP-712 signed, so `NotFound` is an authoritative, attributable
/// claim that the blob does not exist. Signing it during a transient S3 outage
/// tells paying clients to stop asking for a blob the node will serve fine once
/// the origin recovers. The blob's absence from the store is indistinguishable
/// from a clean miss without the engine's error classification — which is exactly
/// what the bare-`bool` fill helpers used to discard.
#[tokio::test(flavor = "multi_thread")]
async fn local_origin_hard_fault_is_internal_error_not_signed_not_found() -> anyhow::Result<()> {
    let payload = vec![0x7Eu8; 64 * 1024];
    let (cache, hash, origin_hits, _cache_tmp) = empty_cache_with_failing_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task, metrics) =
        spawn_fault_server(cache, Arc::clone(&store), FaultTiers::LocalOnly).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    match stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(_) => anyhow::bail!("a hard origin fault must not deliver"),
        Err(e) => {
            let msg = e.to_string();
            anyhow::ensure!(
                msg.contains("InternalError"),
                "a hard origin fault must surface as a retryable InternalError, got: {e}"
            );
            anyhow::ensure!(
                !msg.contains("NotFound"),
                "a hard origin fault must NOT sign an authoritative NotFound, got: {e}"
            );
        }
    }
    anyhow::ensure!(
        origin_hits.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the failing origin should have been consulted (proving the fault came from \
         the reactive fill, not a short-circuit before it)"
    );
    // The operator-facing half: the reject is metered as an internal error, NOT as a
    // cache miss. Without this the wire assertion above still passes while the
    // dashboard reports an origin outage as an empty cache.
    assert_reject_reason(&metrics, 1, 0)?;

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// #1129, the arm most likely to regress: with node→node ON, a hard fault on the
/// LOCAL tier must survive the fall-through to the node→node window tier.
///
/// Falling through after a local fault is correct — a peer may legitimately still
/// serve. But the window origin here is unprovisioned (a dead peer path), so it
/// cleanly misses; the terminal refusal must still be `InternalError`, because a
/// later clean miss must not launder the earlier fault back into a signed
/// `NotFound`. This is what `fault_seen` threading exists to prevent.
#[tokio::test(flavor = "multi_thread")]
async fn local_hard_fault_survives_fallthrough_to_the_window_tier() -> anyhow::Result<()> {
    let payload = vec![0x3Du8; 64 * 1024];
    let (cache, hash, origin_hits, _cache_tmp) = empty_cache_with_failing_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task, metrics) =
        spawn_fault_server(cache, Arc::clone(&store), FaultTiers::LocalAndWindow).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    match stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(_) => anyhow::bail!("a hard origin fault must not deliver"),
        Err(e) => {
            let msg = e.to_string();
            anyhow::ensure!(
                msg.contains("InternalError"),
                "a local hard fault must still surface as InternalError after the \
                 node→node tier cleanly misses, got: {e}"
            );
            anyhow::ensure!(
                !msg.contains("NotFound"),
                "the window tier's clean miss must not overwrite the local fault with \
                 a signed NotFound, got: {e}"
            );
        }
    }
    anyhow::ensure!(
        origin_hits.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the failing local origin should have been consulted before the window tier"
    );
    assert_reject_reason(&metrics, 1, 0)?;

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// #1129 on the BUFFERED node→node tier (`try_pull_through`). Without this test,
/// reverting that helper's `HardFault` arm to a clean miss breaks nothing: every
/// other fault test drives `try_local_populate`.
///
/// This is the tier a resumed request or an unset window provider lands on,
/// and its outcome also has to compose with the latch at the call site
/// (`miss_reason(fault_seen || buffered.is_fault())`) — the `buffered.is_fault()`
/// side of that `||` is exercised nowhere else.
#[tokio::test(flavor = "multi_thread")]
async fn buffered_pull_through_hard_fault_is_internal_error() -> anyhow::Result<()> {
    let payload = vec![0x9Cu8; 64 * 1024];
    let (cache, hash, origin_hits, _cache_tmp) = empty_cache_with_failing_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task, metrics) =
        spawn_fault_server(cache, Arc::clone(&store), FaultTiers::Buffered).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx = channel_context(Arc::clone(&signer), deposit).with_client_binding(binding);

    match stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    {
        Ok(_) => anyhow::bail!("a hard origin fault must not deliver"),
        Err(e) => {
            let msg = e.to_string();
            anyhow::ensure!(
                msg.contains("InternalError"),
                "a buffered-tier hard fault must surface as InternalError, got: {e}"
            );
            anyhow::ensure!(
                !msg.contains("NotFound"),
                "a buffered-tier hard fault must not report the node as merely empty, got: {e}"
            );
        }
    }
    anyhow::ensure!(
        origin_hits.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the failing origin should have been consulted by the buffered tier"
    );
    assert_reject_reason(&metrics, 1, 0)?;

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}
