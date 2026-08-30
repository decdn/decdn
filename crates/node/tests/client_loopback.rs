//! Two-endpoint loopback tests for `cdn/client/v1` paid delivery (#317).
//!
//! Spins up a server running [`ClientHandler`], connects a client endpoint over
//! iroh on localhost, and drives the [`stream_fetch`] requester against it. The
//! happy path proves bytes are delivered + hash-verified and the persisted
//! channel state advances; the error paths prove a zero-rate response is
//! rejected on receive (#252), an unknown channel is cleanly rejected with
//! `VoucherRejected { WrongChannel }` (the #327 boundary), and a transient
//! persist-write failure cleanly ABORTS the stream (ADR 003 §332) — no in-band
//! reject reason, just a clean finish — so the client resends the same voucher
//! on a fresh stream.
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
//! Hostile-client and hostile-server paths (#1562) reimplement the receive or
//! serve loop by hand — [`stall_delivery_at_closing_voucher`] is the honest half
//! the payment-fault tests extend, and [`serve_lying_stream`] is the hostile
//! server the client-side detection tests drive the real [`stream_fetch`]
//! against. Covered here: an underpaying closing voucher fails the stream and
//! advances no watermark; a `BadSignature` and a watermark-regression
//! (`BytesRegression`) proof arriving *after* an accepted voucher are both
//! rejected in band while the accepted watermark survives; the per-connection
//! stream-cap sheds an over-cap stream with a bare QUIC reset and no signed
//! `StreamResponse`; the buyer rejects a server that over-sends past the promised
//! length or delivers hash-mismatched bytes; and the request-read and
//! voucher-read timeouts each tear down a peer that opens a stream (or parks a
//! paid delivery) and then goes silent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_cache::{
    CacheEngine, CacheMetrics, CircuitBreakerPolicy, Hash, PinnedHashes, RetryPolicy,
};
use decdn_incentive::{
    EPHEMERAL_BINDING_NONCE, LaneKey, LaneState, MemoryPoolStateStore, PoolStateStore, Voucher,
    bind_node_id_domain, binding_signing_hash, min_payment, signed_to_wire_voucher,
    slash_judge_domain, stream_sig::StreamSlashData, voucher_domain,
};
use decdn_node::channel_store::PersistentPoolStateStore;
use decdn_node::client_requester::{
    Cumulative, HashMismatch, PoolContext, PoolLedger, PullDeadlines, RateAboveCeiling,
    UpstreamVoucherRejected, VoucherProgress, sign_client_binding, stream_fetch,
    stream_fetch_shared, stream_fetch_tracked, stream_fetch_tracked_with_progress,
};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::client::{ClientHandler, ClientHandlerDeps};
use decdn_node::metrics::Metrics;
use decdn_protocol::client::{
    CHUNK_BYTES, ChunkData, ClientBinding, ClientMessage, StreamError, StreamRequest,
    StreamRequestExt, StreamResponse, StreamResponseBody, StreamResponseExt, VoucherRejectReason,
    encode_stream_response,
};
use decdn_protocol::{ALPN_CLIENT, decode_message, encode_stream_request, read_frame, write_frame};
use iroh::endpoint::{
    ApplicationClose, Connection, ConnectionError, ReadError, ReadToEndError, RecvStream,
    SendStream, VarInt,
};
use iroh::{Endpoint, EndpointAddr};

mod support;
use decdn_node::receipt_log::{DownloadReceipt, spawn_receipt_writer};
use support::{
    BlockingReceiptLog, FailingReceiptLog, HandlerDomains, VecReceiptLog, build_handler_full,
    build_handler_full_configured, build_handler_full_with_receipts, build_handler_full_with_sink,
    cache_with_blob, empty_cache, fresh_key, local_endpoint, permissive_limiter, read_client_msg,
    read_stream_response, shutdown, spawn_server, write_client_msg,
};

const CHAIN_ID: u64 = 421_614;
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

const fn pool_id() -> B256 {
    B256::repeat_byte(0xC1)
}

/// Fixed server-operator identity. The seller keys a lane on the serving node's
/// own operator address (`self.eth_signer.address()`), so a lane seeded before
/// the server is built must name the same key the server later runs with. The
/// loopback servers therefore run this fixed key rather than a random one, and
/// every lane / voucher `provider` in the suite is [`operator_addr`].
#[allow(clippy::expect_used)] // fixed-constant key; a bad scalar is a test bug
fn operator_signer() -> Arc<PrivateKeySigner> {
    Arc::new(
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x42))
            .expect("0x42-repeated is a valid secp256k1 scalar"),
    )
}

/// Address of the fixed [`operator_signer`] — the `provider` every loopback lane
/// pays and every voucher names.
fn operator_addr() -> Address {
    operator_signer().address()
}

/// The lane key a loopback voucher signed by `signer` targets: the fixed
/// [`pool_id`], the paying `signer`, and the [`operator_addr`] provider.
fn lane_key(signer: Address) -> LaneKey {
    LaneKey {
        pool_id: pool_id(),
        signer,
        provider: operator_addr(),
    }
}

/// A fresh lane on [`pool_id`]: `signer` pays [`operator_addr`] with capability
/// cap `cap`, no prior watermark and no expiry.
fn fresh_lane(signer: Address, cap: U256) -> LaneState {
    LaneState::hydrate(
        pool_id(),
        signer,
        operator_addr(),
        cap,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    )
}

/// Build a `ClientHandler` with the given rate and channel store, an unlimited
/// blob-size gate, and a 16-stream per-connection cap (the common-case setup).
fn build_handler(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
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
    store: Arc<dyn PoolStateStore>,
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
    store: Arc<dyn PoolStateStore>,
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
    store: Arc<dyn PoolStateStore>,
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

/// The honest-requester [`PoolContext`]: a fresh lane for `client_signer` paying
/// [`operator_addr`], carrying an ADR 005 client identity binding over the
/// paying `client_ep`'s node id. The pool-model serve gate refuses any request
/// that cannot prove ownership of a lane, so every honest paid fetch attaches a
/// binding signed by the paying key over the connection's node id.
#[allow(clippy::expect_used)] // signing a fixed binding cannot fail in-test
fn channel_context(
    client_ep: &Endpoint,
    client_signer: Arc<PrivateKeySigner>,
    deposit: U256,
) -> PoolContext {
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&client_signer, own_node_id, &binding_domain())
        .expect("sign client binding over the loopback client node id");
    PoolContext {
        pool_id: pool_id(),
        provider: operator_addr(),
        deposit,
        client_signer,
        voucher_domain: payment_domain(),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: Some(binding),
        capability: None,
    }
}

/// The UNBOUND requester context (`client_binding: None`): the honest requester
/// sends no ADR 005 identity binding, so the seller cannot resolve a lane signer
/// and refuses the serve. Used by the control tests that prove the binding is
/// what unlocks the paid path.
fn unbound_context(client_signer: Arc<PrivateKeySigner>, deposit: U256) -> PoolContext {
    PoolContext {
        pool_id: pool_id(),
        provider: operator_addr(),
        deposit,
        client_signer,
        voucher_domain: payment_domain(),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(wire),
        "bytes_delivered: {} (expected {wire})",
        only.last_bytes_delivered()
    );
    anyhow::ensure!(only.last_amount() > U256::ZERO, "amount must be non-zero");

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// One voucher accounting interval — [`decdn_protocol::client::CHUNK_BYTES`].
const HARNESS_INTERVAL_BYTES: u64 = decdn_protocol::client::CHUNK_BYTES;

/// Spin up a serving `ClientHandler` with an explicit downstream credit-window
/// ceiling and ramp divisor (ADR 003 §Credit window, #1669). The effective
/// window at any point is `ramped_credit_window(credit_ramp_divisor,
/// interval, credit_max, paid)`: at `paid = 0` a non-zero divisor floors the
/// window to one interval (stop-and-wait), while `credit_ramp_divisor = 0`
/// opens the full `credit_max` immediately regardless of payment.
async fn spawn_pipelined_server(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    credit_max: u64,
    credit_ramp_divisor: u64,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<PrivateKeySigner>,
    Endpoint,
    tokio::task::JoinHandle<()>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
            deps.credit_max = credit_max;
            deps.credit_ramp_divisor = credit_ramp_divisor;
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth, server_ep, server_task))
}

/// Open a paid `cdn/client/v1` stream and read past its signed `StreamResponse`,
/// leaving the caller positioned to read `ChunkData`. The raw counterpart to the
/// pipelined requester (#1484): it drives the byte stream by hand so a test can
/// choose exactly when (and whether) to pay.
async fn open_paid_stream(
    conn: &Connection,
    hash: [u8; 32],
    ext: Option<&StreamRequestExt>,
) -> anyhow::Result<(SendStream, RecvStream)> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let req = StreamRequest {
        hash,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    let payload =
        encode_stream_request(&req, ext).map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write request: {e}"))?;
    let (resp, resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error);
    Ok((send, recv))
}

/// Read EXACTLY `expect` wire bytes of `ChunkData`, giving each read a generous
/// ceiling so a loaded CI box cannot spuriously truncate a delivery that is in
/// fact coming. Bails if the server sends a non-chunk, stalls before `expect`, or
/// overshoots the expected count. Deterministic under load: it waits for bytes
/// that a correct server WILL send, rather than inferring "done" from a quiet gap.
async fn read_exact_chunks(recv: &mut RecvStream, expect: u64) -> anyhow::Result<()> {
    let mut got: u64 = 0;
    while got < expect {
        let msg = tokio::time::timeout(Duration::from_secs(10), read_client_msg(recv))
            .await
            .map_err(|_| {
                anyhow::anyhow!("server stalled after {got} of {expect} expected wire bytes")
            })??;
        match msg {
            ClientMessage::ChunkData(chunk) => {
                got = got.saturating_add(chunk.bytes().len() as u64);
            }
            other => anyhow::bail!("expected ChunkData, got {other:?}"),
        }
    }
    anyhow::ensure!(
        got == expect,
        "read {got} wire bytes, expected exactly {expect} (server overshot the credit window)"
    );
    Ok(())
}

/// Assert the server has PARKED — that it has sent nothing beyond what was already
/// read and is blocked in `collect_voucher` awaiting a voucher we are withholding.
///
/// This is the credit-exposure bound, checked as an ABSENCE and therefore robust
/// under parallel test load: a correct pipelining node cannot send past the credit
/// window without a voucher, so no further byte is ever coming and the read times
/// out deterministically. Only a buggy OVER-delivering server makes this read
/// return — and it returns fast, so the timeout only bounds how long we wait to
/// catch that bug, never whether a correct server passes.
async fn assert_parked_awaiting_voucher(recv: &mut RecvStream) -> anyhow::Result<()> {
    match tokio::time::timeout(Duration::from_secs(2), read_client_msg(recv)).await {
        Err(_elapsed) => Ok(()),
        Ok(Ok(ClientMessage::ChunkData(chunk))) => anyhow::bail!(
            "server sent {} more wire bytes past the credit window instead of parking for a voucher",
            chunk.bytes().len()
        ),
        Ok(other) => anyhow::bail!("expected the server to park awaiting a voucher; got {other:?}"),
    }
}

/// Sign a single cumulative voucher paying for `bytes_delivered` bytes and
/// write it to `send`. Acceptance is implicit — the node sends no ack, it
/// just advances `paid` and (if the wider window has room) keeps delivering.
async fn pay_cumulative(
    send: &mut SendStream,
    signer: &PrivateKeySigner,
    bytes_delivered: u64,
) -> anyhow::Result<()> {
    let amount = min_payment(bytes_delivered, RATE_PER_MB);
    let voucher = Voucher {
        pool_id: pool_id(),
        signer: signer.address(),
        provider: operator_addr(),
        amount,
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(signer, &payment_domain())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client_msg(
        send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)?),
    )
    .await
}

/// The payer's per-lane seed for the `PayWord` tests. A real seed is derived per
/// `(pool_id, signer, provider)` lane from the payer's key; one is enough here,
/// because every test drives a single lane.
const CHAIN_SEED: B256 = B256::repeat_byte(0x5E);

fn chain_root() -> B256 {
    decdn_incentive::root_from_seed(CHAIN_SEED)
}

/// Sign a **metering** voucher: it opens (or re-asserts) a chain at `root`, and
/// commits to the node's quoted per-MB rate as the chunk price. Every reveal
/// that follows inherits that price, which is why the node checks it here.
async fn open_chain(
    send: &mut SendStream,
    signer: &PrivateKeySigner,
    root: B256,
    chunk_price: u64,
    amount: U256,
    bytes_delivered: u64,
) -> anyhow::Result<()> {
    let voucher = Voucher {
        pool_id: pool_id(),
        signer: signer.address(),
        provider: operator_addr(),
        amount,
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: root,
        chunk_price: U256::from(chunk_price),
    }
    .sign(signer, &payment_domain())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client_msg(
        send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)?),
    )
    .await
}

/// Release the preimage at `index` — the one-message, no-signature tick that
/// pays for one whole chunk.
async fn release(send: &mut SendStream, index: u8) -> anyhow::Result<()> {
    write_client_msg(
        send,
        &ClientMessage::ChunkPreimage(decdn_protocol::client::ChunkPreimage {
            preimage: decdn_incentive::preimage_at(CHAIN_SEED, index).into(),
            index,
        }),
    )
    .await
}

/// Read the mid-stream rejection the node answers a bad proof with, and assert
/// its reason. A rejection rides in band as a `StreamError` and closes the
/// stream cleanly — no QUIC reset — precisely so the payer can tell a payment
/// refusal from a network failure.
async fn expect_reject(recv: &mut RecvStream, expected: VoucherRejectReason) -> anyhow::Result<()> {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), read_client_msg(recv))
            .await
            .map_err(|_| anyhow::anyhow!("node never answered with a rejection"))??;
        match msg {
            // Chunks already in flight when we sent the bad proof.
            ClientMessage::ChunkData(_) => {}
            ClientMessage::StreamError(StreamError::VoucherRejected { reason, bundle }) => {
                anyhow::ensure!(reason == expected, "expected {expected:?}, got {reason:?}");
                anyhow::ensure!(
                    bundle.is_none(),
                    "a chain rejection carries no watermark bundle: a preimage has no \
                     signature of its own, so a payment watermark cannot repair it"
                );
                return Ok(());
            }
            other => anyhow::bail!("expected a VoucherRejected, got {other:?}"),
        }
    }
}

/// The whole `PayWord` tick, end to end: one signed voucher opens a chain, and
/// from then on each delivered chunk is paid for by a single 33-byte reveal with
/// no signature at all. The node credits the reveal exactly as it would a
/// voucher, so the credit window reopens and delivery continues.
#[tokio::test(flavor = "multi_thread")]
async fn a_released_preimage_pays_for_a_chunk_and_resumes_delivery() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x44u8; 8 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    // One chunk delivered against the ramp floor, then the node parks.
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;

    // Anchor this stream to the chain, then pay the delivered chunk with a
    // reveal. The anchor voucher itself credits nothing — it re-asserts a
    // cumulative of zero — so the node keeps waiting until the reveal lands.
    open_chain(&mut send, &signer, chain_root(), RATE_PER_MB, U256::ZERO, 0).await?;
    release(&mut send, 1).await?;

    // The reveal paid for a whole chunk, so the window reopened.
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;

    // And it is recorded, on the same lane row a signature would advance.
    let lane = store
        .get(lane_key(signer.address()))?
        .ok_or_else(|| anyhow::anyhow!("lane row missing"))?;
    anyhow::ensure!(
        lane.chain().verified_index == 1,
        "the lane's chain frontier must record the reveal"
    );
    anyhow::ensure!(
        lane.owed() == U256::from(RATE_PER_MB),
        "one chunk at the quoted rate, owed on top of a zero anchor: {}",
        lane.owed()
    );

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A reveal is a bearer proof: 33 bytes naming no chain. On a stream that has
/// not carried the epoch's root voucher the node cannot say which chain it
/// extends, so it refuses rather than guess. Not fatal — the payer sends the
/// root voucher on this stream and resends.
#[tokio::test(flavor = "multi_thread")]
async fn a_preimage_before_its_epoch_voucher_is_unanchored() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x45u8; 4 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    // A reveal with no anchor on this stream.
    release(&mut send, 1).await?;
    expect_reject(&mut recv, VoucherRejectReason::UnanchoredPreimage).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Index 0 names the chain root and resolves to the voucher's own `amount`, so
/// it proves nothing the voucher does not already say. It is the settlement case
/// at redemption and never travels the wire.
#[tokio::test(flavor = "multi_thread")]
async fn a_preimage_at_index_zero_is_refused() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x46u8; 4 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    open_chain(&mut send, &signer, chain_root(), RATE_PER_MB, U256::ZERO, 0).await?;
    release(&mut send, 0).await?;
    expect_reject(&mut recv, VoucherRejectReason::ChainIndexZero).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A value from another chain never reaches the node's tip, and it cannot: only
/// the payer can produce a value at a given depth. The rejection is terminal —
/// a wrong seed or a wrong chain is a payer bug, not transient state.
#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_preimage_is_refused() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x47u8; 4 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    open_chain(&mut send, &signer, chain_root(), RATE_PER_MB, U256::ZERO, 0).await?;
    write_client_msg(
        &mut send,
        &ClientMessage::ChunkPreimage(decdn_protocol::client::ChunkPreimage {
            preimage: decdn_incentive::preimage_at(B256::repeat_byte(0x5F), 1).into(),
            index: 1,
        }),
    )
    .await?;
    expect_reject(&mut recv, VoucherRejectReason::BadPreimage).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The node MUST check the price it is being paid. A reveal carries no price of
/// its own, so a chain opened at the wrong `chunk_price` would meter every later
/// chunk below the node's quote with no per-tick moment revealing it.
#[tokio::test(flavor = "multi_thread")]
async fn a_chain_opened_below_the_quoted_rate_is_refused() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x48u8; 4 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    // A tenth of the quote — the governance-floor price against a node quoting
    // ten times it.
    open_chain(&mut send, &signer, chain_root(), 1, U256::ZERO, 0).await?;
    expect_reject(&mut recv, VoucherRejectReason::ChunkPriceMismatch).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A sealed voucher meters no chunk, so it MUST carry a zero price — that is
/// what keeps `chain_root == 0 ⟺ chunk_price == 0` true on both sides of the
/// wire, and it is why nothing downstream needs a branch on the zero root.
#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_voucher_carrying_a_price_is_refused() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x49u8; 4 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    open_chain(
        &mut send,
        &signer,
        B256::ZERO,
        RATE_PER_MB,
        min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB),
        HARNESS_INTERVAL_BYTES,
    )
    .await?;
    expect_reject(&mut recv, VoucherRejectReason::ChunkPriceMismatch).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Deepest-preimage-wins, across two concurrent streams on ONE lane.
///
/// The chain's scope is the lane, so both streams share one root and one index
/// space. Stream A skips ahead to index 3 and stream B then offers 1 and 2 —
/// both already covered, so both advance nothing and hash nothing. Stream B's
/// index 5 does advance. The lane converges on the deepest index either stream
/// proved, never on their sum: a claim is a maximum, because a rollover voucher
/// already folds the retired chain into its own amount.
#[tokio::test(flavor = "multi_thread")]
async fn two_streams_on_one_lane_converge_on_the_deepest_index() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x4Au8; 8 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;

    let (mut send_a, mut recv_a) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    let (mut send_b, mut recv_b) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    read_exact_chunks(&mut recv_a, HARNESS_INTERVAL_BYTES).await?;
    read_exact_chunks(&mut recv_b, HARNESS_INTERVAL_BYTES).await?;

    // Each stream anchors ITSELF to the epoch — a sibling having done so is not
    // enough, because a bare reveal is placed against the stream it arrives on.
    open_chain(
        &mut send_a,
        &signer,
        chain_root(),
        RATE_PER_MB,
        U256::ZERO,
        0,
    )
    .await?;
    open_chain(
        &mut send_b,
        &signer,
        chain_root(),
        RATE_PER_MB,
        U256::ZERO,
        0,
    )
    .await?;

    // A skips ahead; B then offers indices already covered, and finally a deeper
    // one. The already-covered reveals are benign, not rejections.
    release(&mut send_a, 3).await?;
    read_exact_chunks(&mut recv_a, HARNESS_INTERVAL_BYTES).await?;
    release(&mut send_b, 1).await?;
    release(&mut send_b, 2).await?;
    release(&mut send_b, 5).await?;
    read_exact_chunks(&mut recv_b, HARNESS_INTERVAL_BYTES).await?;

    let lane = store
        .get(lane_key(signer.address()))?
        .ok_or_else(|| anyhow::anyhow!("lane row missing"))?;
    anyhow::ensure!(
        lane.chain().verified_index == 5,
        "the lane converges on the deepest index proved, got {}",
        lane.chain().verified_index
    );
    anyhow::ensure!(
        lane.owed() == U256::from(5 * RATE_PER_MB),
        "the lane's claim is a MAXIMUM over the streams, never a sum: {}",
        lane.owed()
    );

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1669: with the ramp enabled (a non-zero `credit_ramp_divisor`), a stream that
/// has paid nothing is served only the floor — one chunk — and then
/// parks, exactly the pre-ramp stop-and-wait cadence. `credit_max` being large
/// makes no difference at `paid = 0`: the window is `ramped_credit_window`
/// clamped to the floor until payment clears it.
#[tokio::test(flavor = "multi_thread")]
async fn unpaid_stream_is_served_only_the_floor_then_pauses() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x22u8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (_send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    // Exactly one interval — the ramp floor — then a park.
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1669: as cumulative `paid` advances, the window ramps past the floor once
/// `paid` clears `credit_ramp_divisor * floor`. With `credit_ramp_divisor = 1`
/// the window equals `paid`: after the first interval is paid the window is
/// still pinned at the floor (`paid == floor`, not yet exceeding it), but after
/// the SECOND interval is paid (`paid == 2 * floor`) the window doubles, and the
/// server delivers two further intervals in one pass instead of one before it
/// parks again — the observable widening this test asserts.
#[tokio::test(flavor = "multi_thread")]
async fn paying_grows_the_window_to_paid_over_divisor() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    let payload = vec![0x33u8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 1).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    // Round 1: floor only (paid == 0 during this delivery pass).
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;
    pay_cumulative(&mut send, &signer, HARNESS_INTERVAL_BYTES).await?;

    // Round 2: still floor-width — `paid == 1 * floor` does not yet exceed the
    // floor at divisor 1.
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;
    pay_cumulative(&mut send, &signer, 2 * HARNESS_INTERVAL_BYTES).await?;

    // Round 3: `paid == 2 * floor` now exceeds the floor, so the window is
    // `paid / 1 == 2 * floor` — the server delivers TWO intervals in this pass
    // before parking again, the widened window.
    read_exact_chunks(&mut recv, 2 * HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1669: `credit_ramp_divisor = 0` opens the full `credit_max` ceiling
/// immediately, regardless of `paid` — the flat-window behavior, now opt-in. A
/// client that pays nothing still reads a full `credit_max` ahead of any
/// voucher, and never more than that (the bounded credit exposure).
#[tokio::test(flavor = "multi_thread")]
async fn divisor_zero_serves_the_full_credit_max_immediately() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 8 * 1024 * 1024;
    let payload = vec![0x11u8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 0).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (_send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    // The full ceiling arrives ahead of ANY voucher...
    read_exact_chunks(&mut recv, CREDIT_MAX).await?;
    // ...and not one byte more (exposure bounded to exactly the ceiling).
    assert_parked_awaiting_voucher(&mut recv).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    store: Arc<MemoryPoolStateStore>,
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
/// under one chunk the server has exactly one (closing) voucher left
/// to collect and is now parked reading it — the stall is a protocol-level
/// rendezvous, not a race, and it holds until the test pays (well inside the
/// handler's 10s `VOUCHER_READ_TIMEOUT`).
async fn stall_delivery_at_closing_voucher(
    conn: &Connection,
    hash: [u8; 32],
    expected_wire: u64,
    ext: Option<&StreamRequestExt>,
) -> anyhow::Result<StalledDelivery> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = StreamRequest {
        hash,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    let payload =
        encode_stream_request(&req, ext).map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write request: {e}"))?;

    let (resp, resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error);

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
            pool_id: pool_id(),
            signer: signer.address(),
            provider: operator_addr(),
            // Exactly the advertised-rate minimum for the bytes served, which
            // clears the handler's per-delta `verify_rate` (1% tolerance). The
            // cumulative rate-floor check is inert here: the fixture builds the
            // handler with `delivery_floor = 0`.
            amount: settled.amount,
            bytes_delivered: U256::from(settled.wire_bytes),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(signer, &payment_domain())
        .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
        write_client_msg(
            &mut self.send,
            &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)?),
        )
        .await?;

        // Acceptance is implicit — the server sends no ack and proceeds straight
        // to `StreamEnd` once the closing voucher clears.
        match read_client_msg(&mut self.recv).await? {
            ClientMessage::StreamEnd => {}
            other => anyhow::bail!("expected StreamEnd, got {other:?}"),
        }
        Ok((Instant::now(), settled))
    }

    /// Sign and send a voucher carrying an explicit cumulative
    /// `(bytes_delivered, amount)`, then read and return the node's next message.
    /// Unlike [`Self::pay_and_finish`] this hands the caller an arbitrary
    /// watermark, so a test can inject a malformed or out-of-order voucher and
    /// assert how the node rejects it.
    async fn send_cumulative_voucher(
        &mut self,
        signer: &PrivateKeySigner,
        bytes_delivered: u64,
        amount: U256,
    ) -> anyhow::Result<ClientMessage> {
        let voucher = Voucher {
            pool_id: pool_id(),
            signer: signer.address(),
            provider: operator_addr(),
            amount,
            bytes_delivered: U256::from(bytes_delivered),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(signer, &payment_domain())
        .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
        write_client_msg(
            &mut self.send,
            &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)?),
        )
        .await?;
        read_client_msg(&mut self.recv).await
    }
}

/// Drive a raw `cdn/client/v1` request expecting a pre-serve refusal: send the
/// `StreamRequest` and read only the `StreamResponse` — a refused request never
/// emits `ChunkData`, so there is nothing to stall on. Sibling of
/// [`stall_delivery_at_closing_voucher`] for gates (like the per-lane admission
/// cap) that refuse before delivery begins.
async fn open_expecting_refusal(
    conn: &Connection,
    hash: [u8; 32],
    ext: Option<&StreamRequestExt>,
) -> anyhow::Result<(
    decdn_protocol::client::StreamResponse,
    decdn_protocol::client::StreamResponseExt,
)> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = StreamRequest {
        hash,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    let payload =
        encode_stream_request(&req, ext).map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write request: {e}"))?;

    read_stream_response(&mut recv).await
}

/// Open two same-lane streams as concurrently as the harness allows: both
/// `StreamRequest`s are sent before either `StreamResponse` is read, so the
/// server genuinely sees the two opens overlapping rather than sequenced by
/// this test's own await order. Sibling of [`stall_delivery_at_closing_voucher`]
/// and [`open_expecting_refusal`], for the per-lane admission cap's TOCTOU test.
async fn race_two_same_lane_opens(
    conn: &Connection,
    hash_a: [u8; 32],
    hash_b: [u8; 32],
    ext: &StreamRequestExt,
) -> anyhow::Result<(bool, bool)> {
    let (mut send_a, mut recv_a) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi a: {e}"))?;
    let (mut send_b, mut recv_b) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi b: {e}"))?;

    let req_a = StreamRequest {
        hash: hash_a,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    let req_b = StreamRequest {
        hash: hash_b,
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    let payload_a = encode_stream_request(&req_a, Some(ext))
        .map_err(|e| anyhow::anyhow!("encode request a: {e}"))?;
    let payload_b = encode_stream_request(&req_b, Some(ext))
        .map_err(|e| anyhow::anyhow!("encode request b: {e}"))?;

    // Both requests are in flight before either response is read.
    write_frame(&mut send_a, &payload_a)
        .await
        .map_err(|e| anyhow::anyhow!("write request a: {e}"))?;
    write_frame(&mut send_b, &payload_b)
        .await
        .map_err(|e| anyhow::anyhow!("write request b: {e}"))?;

    let ok_a = match read_client_msg(&mut recv_a).await? {
        ClientMessage::StreamResponse(resp) => resp.body.ok,
        other => anyhow::bail!("expected a StreamResponse for a, got {other:?}"),
    };
    let ok_b = match read_client_msg(&mut recv_b).await? {
        ClientMessage::StreamResponse(resp) => resp.body.ok,
        other => anyhow::bail!("expected a StreamResponse for b, got {other:?}"),
    };

    Ok((ok_a, ok_b))
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
    // 64 KiB: far under the fixed voucher accounting interval, so the delivery
    // has exactly one (closing) voucher and the park point is unambiguous.
    let payload = vec![0x3Du8; 64 * 1024];
    let idle = Duration::from_millis(150);
    let fx = idle_fixture(&payload, idle).await?;
    let blob = fx.blob(0)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;
    let stalled = stall_delivery_at_closing_voucher(
        &conn,
        *blob.hash.as_bytes(),
        blob.wire_bytes,
        Some(&ext),
    )
    .await?;

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
        only.last_bytes_delivered() == U256::from(blob.wire_bytes),
        "bytes_delivered: {} (expected {})",
        only.last_bytes_delivered(),
        blob.wire_bytes
    );

    drop(conn);
    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
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
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;
    let stalled = stall_delivery_at_closing_voucher(
        &conn,
        *blob.hash.as_bytes(),
        blob.wire_bytes,
        Some(&ext),
    )
    .await?;

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

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
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
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;
    let mut totals = VoucherTotals::default();
    let mut final_completion = Instant::now();

    for round in 1..=3 {
        let stalled = stall_delivery_at_closing_voucher(
            &conn,
            *blob.hash.as_bytes(),
            blob.wire_bytes,
            Some(&ext),
        )
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
        only.last_bytes_delivered() == U256::from(totals.wire_bytes),
        "bytes_delivered: {} (expected {})",
        only.last_bytes_delivered(),
        totals.wire_bytes
    );
    anyhow::ensure!(
        metric_line_present(&fx.metrics.encode()?, "decdn_client_idle_close_total 1"),
        "repeated re-arms must end in exactly one metered idle-close"
    );

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
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
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;
    let stalled_a = stall_delivery_at_closing_voucher(
        &conn,
        *blob_a.hash.as_bytes(),
        blob_a.wire_bytes,
        Some(&ext),
    )
    .await?;
    let stalled_b = stall_delivery_at_closing_voucher(
        &conn,
        *blob_b.hash.as_bytes(),
        blob_b.wire_bytes,
        Some(&ext),
    )
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
        only.last_bytes_delivered() == U256::from(expected_wire_bytes),
        "persisted bytes: {}, expected {expected_wire_bytes}",
        only.last_bytes_delivered(),
    );
    anyhow::ensure!(
        metric_line_present(&fx.metrics.encode()?, "decdn_client_idle_close_total 1"),
        "the post-concurrency idle reap must be metered exactly once"
    );

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// ADR 005 §Payment lanes and concurrent streams: two same-lane streams share
/// ONE aggregate byte counter and ONE cumulative-voucher watermark. Two
/// deliveries run on one `(pool_id, signer, provider)` lane — each blob is under
/// a single chunk, so neither stream crosses the 1 MiB
/// `CHUNK_BYTES` boundary alone, but their combined wire bytes do — so
/// the SHARED lane counter is what carries the crossing. Both serve legs sit
/// parked at their closing voucher on the one lane, then settle in the REVERSE of
/// their open order. The node reconstructs each voucher's cumulative
/// `bytes_delivered` from the shared counter plus that stream's own delivered
/// delta, so the final persisted watermark is the exact aggregate no matter which
/// stream is paid first (#1689).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_same_lane_streams_aggregate_across_the_chunk_boundary() -> anyhow::Result<()> {
    // Each blob is under one chunk, so a single stream yields just one
    // (closing) voucher; their sum clears the interval, so only the shared lane
    // counter crosses the boundary — the aggregate-accounting path under test.
    // Each blob is under ONE chunk on its own, so neither crosses the boundary
    // by itself — but together they do, which is exactly the lane-aggregate
    // accounting under test.
    let payload_a = vec![0x71u8; 768 * 1024];
    let payload_b = vec![0x82u8; 512 * 1024];
    // A generous idle window: the test settles promptly and asserts nothing about
    // the idle reaper, so the clock must never fire while streams are parked.
    let idle = Duration::from_secs(30);
    let fx = idle_fixture_with_two_blobs(&payload_a, &payload_b, idle).await?;
    let blob_a = fx.blob(0)?;
    let blob_b = fx.blob(1)?;

    // The preconditions the test's meaning rests on: each stream stays under one
    // interval (so neither crosses the boundary on its own), yet their aggregate
    // clears it (so the shared counter must).
    anyhow::ensure!(
        blob_a.wire_bytes < CHUNK_BYTES && blob_b.wire_bytes < CHUNK_BYTES,
        "each blob must stay under one chunk: a={}, b={}, chunk={}",
        blob_a.wire_bytes,
        blob_b.wire_bytes,
        CHUNK_BYTES,
    );
    let aggregate_wire = blob_a
        .wire_bytes
        .checked_add(blob_b.wire_bytes)
        .ok_or_else(|| anyhow::anyhow!("aggregate wire-byte overflow"))?;
    anyhow::ensure!(
        aggregate_wire > CHUNK_BYTES,
        "the two streams must aggregate past one interval: {aggregate_wire} <= {CHUNK_BYTES}",
    );

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;

    // Both streams open on the one connection and fully deliver: every chunk of
    // both blobs is on the wire (each blob sits inside the one-interval credit
    // window) before either voucher is paid, so the two serve legs are
    // simultaneously parked at their closing-voucher exchange on the shared lane.
    let stalled_a = stall_delivery_at_closing_voucher(
        &conn,
        *blob_a.hash.as_bytes(),
        blob_a.wire_bytes,
        Some(&ext),
    )
    .await?;
    let stalled_b = stall_delivery_at_closing_voucher(
        &conn,
        *blob_b.hash.as_bytes(),
        blob_b.wire_bytes,
        Some(&ext),
    )
    .await?;

    // Out-of-order settlement: pay stream B (opened SECOND) first, then A. The
    // cumulative watermark is threaded in PAYMENT order, not open order — the node
    // steps the shared counter by each stream's own delivered delta, so B's
    // voucher reconstructs to `wire_b` and A's to `wire_b + wire_a`.
    let (_b_completed_at, after_b) = stalled_b
        .pay_and_finish(&fx.client_signer, VoucherTotals::default())
        .await?;
    anyhow::ensure!(
        after_b.wire_bytes == blob_b.wire_bytes,
        "after paying B first the cumulative wire bytes must be exactly B's: {} vs {}",
        after_b.wire_bytes,
        blob_b.wire_bytes,
    );

    // A's cumulative voucher is where the shared counter crosses the 1 MiB
    // boundary (from `wire_b` to `wire_b + wire_a`).
    let (_a_completed_at, after_a) = stalled_a.pay_and_finish(&fx.client_signer, after_b).await?;
    anyhow::ensure!(
        after_a.wire_bytes == aggregate_wire,
        "the final cumulative wire bytes must be the exact aggregate: {} vs {aggregate_wire}",
        after_a.wire_bytes,
    );

    // The lane persisted ONE shared watermark — the exact aggregate of both
    // streams (monotone, never double-counted) — and its cumulative amount is what
    // the two vouchers paid.
    let persisted = fx.store.load_all()?;
    anyhow::ensure!(
        persisted.len() == 1,
        "the two streams share one lane, but {} were persisted",
        persisted.len(),
    );
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted lane"))?;
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(aggregate_wire),
        "shared lane watermark: {} (expected the {aggregate_wire}-byte aggregate)",
        only.last_bytes_delivered(),
    );
    anyhow::ensure!(
        only.last_amount() == after_a.amount,
        "cumulative amount: {} (expected {})",
        only.last_amount(),
        after_a.amount,
    );

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// A lane funded for exactly one credit-window floor admits the first same-lane
/// stream and refuses a concurrent second with `NotFound` (the wire collapse of
/// `LaneAtCapacity`), while the admitted stream still delivers and settles.
///
/// `remaining = 50`: covers the first stream's own 3 MiB guard (cost 30, so the
/// base floor-M gate admits it) plus one credit-window floor (`HARNESS_FLOOR_COST
/// = 40`) — but not a second stream's 2 MiB guard (cost 20) stacked on top of the
/// floor already charged for the first, still-active stream (20 + 40 = 60 > 50).
/// This is the exactly-one-floor headroom the admission cap enforces: `remaining`
/// covers `min_payment(floor, rate)` (40) but not `min_payment(guard_b + floor,
/// rate)` (60).
#[tokio::test(flavor = "multi_thread")]
async fn second_same_lane_stream_refused_when_budget_covers_one() -> anyhow::Result<()> {
    // Each blob stays inside ONE chunk of wire, so the whole delivery rides the
    // ramp floor and the test settles it with a single closing voucher.
    let payload_a = vec![0x71u8; 512 * 1024];
    let payload_b = vec![0x82u8; 256 * 1024];
    let (cache, hash_a, hash_b, _cache_tmp) = cache_with_two_blobs(&payload_a, &payload_b).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // One floor of pool headroom and no more: the credit window's ramp floor is
    // one chunk, so a floor costs `min_payment(CHUNK_BYTES, rate)`. Derived
    // rather than hard-coded so it tracks the payment quantum.
    let remaining = min_payment(CHUNK_BYTES, RATE_PER_MB) + U256::from(2u64);
    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        remaining,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = binding_ext(&signer, client_node_id)?;

    // First same-lane stream: admitted, parked mid-delivery (holds its slot).
    let wire_a = support::bao_wire_len_whole(payload_a.len() as u64);
    let first =
        stall_delivery_at_closing_voucher(&conn, *hash_a.as_bytes(), wire_a, Some(&ext)).await?;

    // Second concurrent same-lane stream: refused with NotFound.
    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn, *hash_b.as_bytes(), Some(&ext)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "second same-lane stream must be refused while budget covers only one floor"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "expected the collapsed NotFound wire code, got {:?}",
        refusal_ext.error
    );

    // The admitted stream still settles cleanly once its slot is the only one
    // left, proving the cap released and did not wedge the lane.
    let _ = first
        .pay_and_finish(&signer, VoucherTotals::default())
        .await?;

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A finished stream releases its lane slot: after the first same-lane stream
/// settles and its `LaneSlot` drops, a later same-lane open on the same
/// one-floor budget is admitted again. Uses the exact fixture and `remaining =
/// 50` tuning as `second_same_lane_stream_refused_when_budget_covers_one` above
/// — it proves the counter decrements on release, not merely that a fresh lane
/// admits; a leaked slot would refuse (or wedge, since the first stream never
/// existed to steal capacity from) the second open here.
#[tokio::test(flavor = "multi_thread")]
async fn finished_stream_releases_its_lane_slot() -> anyhow::Result<()> {
    // Each blob stays inside ONE chunk of wire, so the whole delivery rides the
    // ramp floor and the test settles it with a single closing voucher.
    let payload_a = vec![0x71u8; 512 * 1024];
    let payload_b = vec![0x82u8; 256 * 1024];
    let (cache, hash_a, hash_b, _cache_tmp) = cache_with_two_blobs(&payload_a, &payload_b).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // One floor of pool headroom and no more: the credit window's ramp floor is
    // one chunk, so a floor costs `min_payment(CHUNK_BYTES, rate)`. Derived
    // rather than hard-coded so it tracks the payment quantum.
    let remaining = min_payment(CHUNK_BYTES, RATE_PER_MB) + U256::from(2u64);
    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        remaining,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = binding_ext(&signer, client_node_id)?;

    // Admit, deliver, and fully settle the first stream — its `LaneSlot` drops
    // once `pay_and_finish` returns.
    let wire_a = support::bao_wire_len_whole(payload_a.len() as u64);
    let first =
        stall_delivery_at_closing_voucher(&conn, *hash_a.as_bytes(), wire_a, Some(&ext)).await?;
    let (_first_completed_at, totals) = first
        .pay_and_finish(&signer, VoucherTotals::default())
        .await?;

    // A later same-lane open now succeeds on the same budget — the slot was
    // released, not leaked. `stall_delivery_at_closing_voucher` itself asserts
    // `resp.body.ok`, so a leaked slot fails this call with a refusal error.
    // The shared lane's cumulative watermark carries forward (as in
    // `concurrent_same_lane_streams_aggregate_across_the_chunk_boundary`
    // above): the second voucher's `bytes_delivered` must cover both streams.
    let wire_b = support::bao_wire_len_whole(payload_b.len() as u64);
    let second =
        stall_delivery_at_closing_voucher(&conn, *hash_b.as_bytes(), wire_b, Some(&ext)).await?;
    let _ = second.pay_and_finish(&signer, totals).await?;

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A single same-lane stream on a one-floor budget is admitted and settles
/// exactly as before the admission cap — the `n = 1` path applies no
/// surcharge, only the stream's own guard cost.
#[tokio::test(flavor = "multi_thread")]
async fn single_same_lane_stream_admitted_unchanged() -> anyhow::Result<()> {
    // Inside one chunk of wire, so the whole delivery rides the ramp floor.
    let payload = vec![0x93u8; 512 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // One floor of pool headroom and no more: the credit window's ramp floor is
    // one chunk, so a floor costs `min_payment(CHUNK_BYTES, rate)`. Derived
    // rather than hard-coded so it tracks the payment quantum.
    let remaining = min_payment(CHUNK_BYTES, RATE_PER_MB) + U256::from(2u64);
    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        remaining,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = binding_ext(&signer, client_node_id)?;

    let wire = support::bao_wire_len_whole(payload.len() as u64);
    let only = stall_delivery_at_closing_voucher(&conn, *hash.as_bytes(), wire, Some(&ext)).await?;
    let (_completed_at, totals) = only
        .pay_and_finish(&signer, VoucherTotals::default())
        .await?;
    anyhow::ensure!(
        totals.wire_bytes == wire,
        "settled wire bytes: {} (expected the whole blob's {wire})",
        totals.wire_bytes
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Two simultaneous same-lane opens on a one-floor budget: the lane lock
/// serializes admission, so exactly one is admitted and the other refused with
/// `NotFound` — no TOCTOU double-admit. Both requests fit the budget alone
/// (guards 30 and 20 under `remaining = 50`), so admitting both would only be
/// possible if the two opens raced past the gate without serializing; which one
/// wins is scheduling-dependent and is deliberately not asserted.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_opens_admit_exactly_one() -> anyhow::Result<()> {
    // Each blob stays inside ONE chunk of wire, so the whole delivery rides the
    // ramp floor and the test settles it with a single closing voucher.
    let payload_a = vec![0x71u8; 512 * 1024];
    let payload_b = vec![0x82u8; 256 * 1024];
    let (cache, hash_a, hash_b, _cache_tmp) = cache_with_two_blobs(&payload_a, &payload_b).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // One floor of pool headroom and no more: the credit window's ramp floor is
    // one chunk, so a floor costs `min_payment(CHUNK_BYTES, rate)`. Derived
    // rather than hard-coded so it tracks the payment quantum.
    let remaining = min_payment(CHUNK_BYTES, RATE_PER_MB) + U256::from(2u64);
    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        remaining,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = binding_ext(&signer, client_node_id)?;

    let (a_ok, b_ok) =
        race_two_same_lane_opens(&conn, *hash_a.as_bytes(), *hash_b.as_bytes(), &ext).await?;
    assert_ne!(
        a_ok, b_ok,
        "exactly one of the two concurrent opens is admitted"
    );

    // The admitted stream's chunks are left unread here — this test only
    // exercises the gate, not delivery — so the server side may still be writing
    // when the endpoints close. `spawn_server` swallows handler errors, so that
    // reaches nothing below; only a panic would.
    drop(conn);
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A SKIP-AHEAD voucher — one whose cumulative `bytes_delivered` assumes
/// another same-lane stream's bytes are already settled, sent before that
/// stream is paid — is accepted. The voucher's cumulative bytes come from the
/// WIRE (ADR 005 §Voucher wire format), self-described and signed by the
/// client, so the node verifies it directly against the lane's pinned signer
/// instead of reconstructing a per-stream cumulative from the shared lane
/// counter plus this stream's delivered delta. That reconstruction was what
/// made a skip-ahead voucher recover a different address than it was signed
/// under; with the wire value verified directly, settlement no longer depends
/// on the order concurrent same-lane streams pay in.
#[tokio::test(flavor = "multi_thread")]
async fn skip_ahead_voucher_on_a_concurrent_lane_is_accepted() -> anyhow::Result<()> {
    // Both blobs sit under one chunk, so each stream has a single
    // closing voucher; their sizes differ so a skip-ahead cumulative cannot
    // coincidentally match the reconstructed per-stream value.
    let payload_a = vec![0x71u8; 512 * 1024];
    let payload_b = vec![0x82u8; 384 * 1024];
    let idle = Duration::from_secs(30);
    let fx = idle_fixture_with_two_blobs(&payload_a, &payload_b, idle).await?;
    let blob_a = fx.blob(0)?;
    let blob_b = fx.blob(1)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;

    // Both streams deliver fully and park at their closing voucher; nothing is
    // paid yet, so the shared lane counter is still 0. Stream A is left parked
    // (never paid) for the whole test — its bytes are exactly what B's skip-ahead
    // voucher wrongly assumes are already on the lane.
    let _stalled_a = stall_delivery_at_closing_voucher(
        &conn,
        *blob_a.hash.as_bytes(),
        blob_a.wire_bytes,
        Some(&ext),
    )
    .await?;
    let mut stalled_b = stall_delivery_at_closing_voucher(
        &conn,
        *blob_b.hash.as_bytes(),
        blob_b.wire_bytes,
        Some(&ext),
    )
    .await?;

    // On stream B, pay a voucher that SKIPS AHEAD: its cumulative bytes/amount
    // assume A's bytes are already on the lane, even though A has not been paid.
    let skip_ahead_bytes = blob_a
        .wire_bytes
        .checked_add(blob_b.wire_bytes)
        .ok_or_else(|| anyhow::anyhow!("skip-ahead wire-byte overflow"))?;
    let skip_ahead_amount = min_payment(blob_a.wire_bytes, RATE_PER_MB)
        .checked_add(min_payment(blob_b.wire_bytes, RATE_PER_MB))
        .ok_or_else(|| anyhow::anyhow!("skip-ahead amount overflow"))?;

    let reply = stalled_b
        .send_cumulative_voucher(&fx.client_signer, skip_ahead_bytes, skip_ahead_amount)
        .await?;

    // Self-describing bytes: B's voucher carries the aggregate cumulative it
    // was signed over, so the node verifies it directly and advances the lane
    // watermark to A+B — the skip-ahead voucher is accepted, not WrongSigner.
    match reply {
        ClientMessage::StreamError(err) => {
            anyhow::bail!("skip-ahead voucher must be accepted, got StreamError {err:?}")
        }
        // A clean acceptance surfaces as continued delivery ending in StreamEnd
        // (B's whole range is already on the wire), or no immediate reply frame.
        ClientMessage::StreamEnd | ClientMessage::ChunkData(_) => {}
        other => anyhow::bail!("unexpected reply to an accepted voucher: {other:?}"),
    }

    // The accepted aggregate is the persisted watermark.
    let persisted = fx.store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("lane state must persist after an accepted voucher"))?;
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(skip_ahead_bytes),
        "an accepted skip-ahead voucher lands the aggregate watermark, got {}",
        only.last_bytes_delivered(),
    );

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// #1699 out-of-order settlement: after a higher (aggregate) voucher settles
/// the lane, a lower voucher arriving on a slower stream is ALREADY-SATISFIED —
/// the node does not kill that stream and the watermark never regresses. This
/// is the case a naive wire+verify change would fail (fatal regression).
#[tokio::test(flavor = "multi_thread")]
async fn lower_voucher_after_higher_sibling_is_already_satisfied() -> anyhow::Result<()> {
    let payload_a = vec![0x71u8; 512 * 1024];
    let payload_b = vec![0x82u8; 384 * 1024];
    let idle = Duration::from_secs(30);
    let fx = idle_fixture_with_two_blobs(&payload_a, &payload_b, idle).await?;
    let blob_a = fx.blob(0)?;
    let blob_b = fx.blob(1)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;

    let mut stalled_a = stall_delivery_at_closing_voucher(
        &conn,
        *blob_a.hash.as_bytes(),
        blob_a.wire_bytes,
        Some(&ext),
    )
    .await?;
    let mut stalled_b = stall_delivery_at_closing_voucher(
        &conn,
        *blob_b.hash.as_bytes(),
        blob_b.wire_bytes,
        Some(&ext),
    )
    .await?;

    // Settle the aggregate (A+B) on stream B first — the "higher" voucher.
    let agg_bytes = blob_a.wire_bytes + blob_b.wire_bytes;
    let agg_amount =
        min_payment(blob_a.wire_bytes, RATE_PER_MB) + min_payment(blob_b.wire_bytes, RATE_PER_MB);
    let reply_b = stalled_b
        .send_cumulative_voucher(&fx.client_signer, agg_bytes, agg_amount)
        .await?;
    anyhow::ensure!(
        !matches!(reply_b, ClientMessage::StreamError(_)),
        "aggregate voucher on B must be accepted, got {reply_b:?}"
    );

    // Now the LOWER standalone voucher for just A arrives on stream A. It is
    // below the A+B watermark — already satisfied, NOT a WrongSigner/regression
    // kill.
    let reply_a = stalled_a
        .send_cumulative_voucher(
            &fx.client_signer,
            blob_a.wire_bytes,
            min_payment(blob_a.wire_bytes, RATE_PER_MB),
        )
        .await?;
    anyhow::ensure!(
        !matches!(reply_a, ClientMessage::StreamError(_)),
        "a superseded lower voucher must be already-satisfied, got StreamError: {reply_a:?}"
    );

    // The watermark holds at the aggregate; it did not regress to A.
    let persisted = fx.store.load_all()?;
    let only = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("lane state must persist"))?;
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(agg_bytes),
        "watermark must stay at the aggregate, got {}",
        only.last_bytes_delivered(),
    );

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
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
    // Empty payload → `hash` is the empty root, computed via `bao_tree::blake3`, by construction.
    let payload: Vec<u8> = Vec::new();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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
        only.last_bytes_delivered() == U256::ZERO,
        "0 bytes delivered, got {}",
        only.last_bytes_delivered()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Watermark-on-error contract (#852) on the `stream_fetch_tracked` path: when a
/// pull errors mid-stream *after* at least one voucher was acked, `progress` must
/// still hold the last acked watermark so the caller can persist what it paid —
/// it must NOT reset to `None`.
///
/// Induced deterministically by deposit exhaustion (no mock server): a blob one
/// chunk plus a remainder needs two vouchers — a cumulative amount
/// for the interval, then a larger cumulative amount for the close — but the
/// channel deposit only clears the first. The node acks voucher 1 and rejects
/// voucher 2 as over-deposit, so the fetch errors after one acked voucher.
/// `progress.advanced()` must then report voucher 1 (nonce 1), proving the
/// copy-back in `stream_fetch_tracked` runs on the error path.
///
/// The deposit is load-bearing in both directions: it must clear the #1516
/// pre-serve gate's ceiling (one credit window at `RATE_PER_MB`) so the first
/// attempt is actually served, and fall short of voucher 2 so the mid-stream
/// ceiling is what stops it. An acked nonce of 1 is only reachable through
/// that exact sequence, so it pins the mid-stream backstop as surely as
/// asserting on the reject reason did.
///
/// The *terminal* error is the voucher rejection itself, and no resume retry is
/// attempted at all. The rejection does carry an authenticated `WatermarkBundle`,
/// but that bundle merely echoes the watermark this client already holds (nonce 1
/// — the node attaches one to every watermark-gated rejection once any voucher has
/// been accepted, including a genuinely exhausted one). `PoolLedger::reseed`
/// refuses a non-advancing cumulative, so `fetch_inner` surfaces the real cause
/// instead of spending a resume attempt re-sending a voucher the node has already
/// refused for lack of deposit. The counter assertion below pins that: pre-#1516
/// each futile attempt was served a fresh free window, and now there is no futile
/// attempt to serve.
#[tokio::test(flavor = "multi_thread")]
async fn tracked_watermark_survives_post_ack_error() -> anyhow::Result<()> {
    // 6 MiB — several whole chunks plus a closing remainder, so the transfer
    // meters a run of reveals and then settles the tail with a signature.
    let payload = vec![0xABu8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    // Exactly voucher 1's cumulative amount: it clears the #1516 pre-serve
    // ceiling (also one interval, with no configured credit window) and covers
    // voucher 1, but falls short of voucher 2's larger cumulative amount.
    let deposit = min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);
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

    // Assert the *intended* failure mode, not just any error: the over-deposit
    // voucher rejection is what surfaces, unmasked by a futile resume. A
    // regression that errors for some other reason (e.g. a transport fault)
    // should fail this test loudly.
    let err = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("fetch must error when voucher 2 is over-deposit"))?;
    let rejected = err
        .downcast_ref::<UpstreamVoucherRejected>()
        .ok_or_else(|| anyhow::anyhow!("expected UpstreamVoucherRejected, got: {err:?}"))?;
    anyhow::ensure!(
        matches!(
            rejected.reason,
            decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted
        ),
        "the exhausted channel must surface its own rejection reason; got {:?}",
        rejected.reason
    );
    // The bundle IS attached — this is the case that made bundle presence alone
    // an unsafe reseed trigger. Pinning it here keeps the test honest: it is
    // asserting that a *present* bundle was correctly declined, not that none
    // arrived.
    anyhow::ensure!(
        rejected.bundle.is_some(),
        "the rejection should still carry a watermark bundle; \
         without one this test would pass vacuously"
    );
    // The contract this test exists for: the watermark survives the error and
    // reflects the one cleared voucher. A non-zero cumulative amount — and no
    // further advance — is only possible if voucher 1 cleared and voucher 2 hit
    // the mid-stream cap ceiling, so this also pins that backstop.
    let advanced = progress.advanced().ok_or_else(|| {
        anyhow::anyhow!("advanced watermark must survive a post-ack error, got None")
    })?;
    anyhow::ensure!(
        advanced.1 > U256::ZERO,
        "at least one voucher should have cleared before the rejection; amount = {}",
        advanced.1
    );
    // #1516 pinned that a futile resume retry is refused pre-serve rather than
    // handed a free credit window. With the non-advancing bundle now declined,
    // there is no second attempt to refuse at all — strictly stronger, so the
    // pre-serve rejection counter must stay at zero.
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 0"
        ),
        "an exhausted channel must not retry at all, so nothing should reach the \
         pre-serve deposit gate"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Wiring guard for `decdn node lanes` (issue #1733): a real signed-voucher
/// accept through the live `ClientHandler` must advance the last-voucher clock
/// the admin surface reads off the lane registry.
///
/// The stamp call site (`handlers/client/voucher.rs`) has no other test caller —
/// a dropped or mis-placed stamp would silently leave `seconds_since_last_voucher`
/// at `None` ("never") forever. Here we take a `LaneActivityClock` over the
/// handler's registry, drive one successful delivery (which accepts vouchers),
/// and assert the lane now reports `Some(age)`. Before the accept the lane's
/// stamp is `0`, so it is absent from `ages`; a present age proves the accept
/// path stamped the lane state.
#[tokio::test(flavor = "multi_thread")]
async fn accepted_voucher_advances_lane_activity_clock() -> anyhow::Result<()> {
    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB → crosses a chunk
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        |_deps| {},
    )?;
    // A read handle over the handler's live registry — the same wiring
    // `runtime::run` performs for the admin surface. Before any accept the
    // lane's stamp is 0, so it is absent from `ages`.
    let activity = handler.lane_activity_clock();
    assert!(
        !activity
            .ages()
            .await
            .contains_key(&lane_key(client_signer.address())),
        "no voucher accepted yet → clock must report the lane as absent (never)"
    );

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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

    // The accept path stamped the lane state: the admin surface reading the
    // same registry would now report an age rather than "never".
    assert!(
        activity
            .ages()
            .await
            .contains_key(&lane_key(client_signer.address())),
        "an accepted voucher must advance the lane's last-voucher clock"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Issue #248: every proof that actually credits bytes appends one download
/// receipt naming the served blob's hash, the bytes it paid for, the paying
/// client's iroh node id, and the lane's cumulative amount.
///
/// Under `PayWord` the meter ticks per chunk, so a multi-chunk delivery records
/// one receipt per whole chunk plus one for the closing residual — and their
/// `size`s still sum to exactly the delivered wire bytes, which is the property
/// the audit log actually rests on. The count is asserted against that sum
/// rather than hard-coded, so it tracks the blob rather than the cadence.
///
/// Crucially the anchor voucher each stream sends before its first reveal
/// records NOTHING: it re-asserts a cumulative the lane already holds, so it
/// credits nothing, and a receipt for it would log a payment that never
/// happened.
#[tokio::test(flavor = "multi_thread")]
async fn voucher_acceptance_appends_download_receipt() -> anyhow::Result<()> {
    // 6 MiB — several whole chunks, plus a closing residual smaller than one.
    let payload = vec![0x5Au8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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

    assert_receipts_cover_the_delivery(&receipts.snapshot(), hash, client_node_id, &payload)?;

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Every assertion the receipt log has to satisfy after a completed delivery,
/// split out to keep the test itself readable.
fn assert_receipts_cover_the_delivery(
    recorded: &[DownloadReceipt],
    hash: Hash,
    client_node_id: iroh::PublicKey,
    payload: &[u8],
) -> anyhow::Result<()> {
    let want_hash = hex_lower(hash.as_bytes());
    let want_node = hex_lower(client_node_id.as_bytes());
    let total: u64 = recorded.iter().map(DownloadReceipt::size).sum();
    // ADR 038: metered quantity is bao wire bytes
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    // One receipt per whole chunk, plus one for the residual when the wire
    // length is not an exact multiple.
    let chunk = decdn_protocol::client::CHUNK_BYTES;
    let expected = (wire / chunk) + u64::from(!wire.is_multiple_of(chunk));
    anyhow::ensure!(
        recorded.len() as u64 == expected,
        "expected {expected} receipts (one per chunk plus the closing residual), got {}: \
         {recorded:?}",
        recorded.len()
    );
    anyhow::ensure!(
        recorded.iter().all(|r| r.size() > 0),
        "a proof that credited nothing must record no receipt: {recorded:?}"
    );
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
    // Proofs carry no nonce; each accepted proof records the lane's cumulative
    // claim at that moment, so the amounts rise strictly across the run — the
    // reveals by one `chunk_price` each, the closing residual by less.
    let amounts: Vec<u128> = recorded
        .iter()
        .map(|r| r.voucher_amount().parse::<u128>().unwrap_or(0))
        .collect();
    anyhow::ensure!(
        amounts.windows(2).all(|w| w.first() < w.last()),
        "receipt amounts must rise strictly, got {amounts:?}"
    );
    anyhow::ensure!(
        amounts.first() == Some(&u128::from(RATE_PER_MB)),
        "the first reveal claims exactly one chunk at the quoted rate, got {amounts:?}"
    );
    Ok(())
}

/// Issue #803: receipt-log I/O must never back-pressure paid delivery. Wired
/// through the REAL `ChannelReceiptSink` + background writer (not the inline
/// `DirectReceiptSink`), with the underlying log stalled on every `append`, the
/// full blob must still deliver and hash-verify — the buggy pre-#803 code
/// awaited the append inline before continuing delivery, so it would hang here. After
/// releasing the stall and draining the writer, every receipt is recovered,
/// proving the decoupling loses nothing on a clean shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn delivery_completes_while_receipt_writer_is_stalled() -> anyhow::Result<()> {
    let payload = vec![0x3Cu8; 1_572_864]; // 1.5 MiB — two voucher appends.
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // The production sink: a bounded channel feeding a background writer whose
    // log blocks on every append until we release it.
    let blocking_log = Arc::new(BlockingReceiptLog::default());
    let (receipt_sink, writer_handle) = spawn_receipt_writer(
        Arc::clone(&blocking_log) as Arc<dyn decdn_node::receipt_log::ReceiptLog>,
        Arc::clone(&metrics),
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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
    tokio::time::timeout(Duration::from_secs(10), writer_handle.shutdown())
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Reused channel: two sequential streams on one channel. The second stream
/// must resume from the first's cumulative voucher state (bytes/amount), not
/// restart at zero — otherwise the node rejects the second voucher as
/// `AmountRegression`. Validates the `PoolContext.prior_*` resume fields.
#[tokio::test(flavor = "multi_thread")]
async fn client_reused_channel_resumes() -> anyhow::Result<()> {
    let payload = vec![0x33u8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx1 = channel_context(&client_ep, Arc::clone(&client_signer), deposit);
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
    // ADR 038: the metered/persisted quantity is the bao verified-stream wire
    // size, not the payload content length.
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        s1.last_bytes_delivered() == U256::from(wire),
        "stream-1 bytes: {} (expected {wire})",
        s1.last_bytes_delivered()
    );

    // Stream 2: resume from the channel's advanced state. Each `stream_fetch`
    // opens a fresh connection, so the resume request re-sends the binding.
    let ctx2 = PoolContext {
        prior_bytes_delivered: s1.last_bytes_delivered(),
        prior_amount: s1.last_amount(),
        ..channel_context(&client_ep, Arc::clone(&client_signer), deposit)
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
        s2.last_bytes_delivered() == U256::from(2 * wire),
        "cumulative bytes: {}",
        s2.last_bytes_delivered()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, Arc::clone(&client_signer), deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, client_signer, deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #327 boundary + #848 free-egress: a stream request for a channel the node has
/// never persisted is refused *pre-serve* — the node signs `ok: false` with the
/// delivery-side `NotFound` code and ships zero bytes. Previously it served up to
/// one chunk (or the whole blob, if smaller) for free and only
/// rejected the voucher mid-stream with `VoucherRejected { WrongChannel }`. The
/// 1.5 MiB blob (larger than the chunk) proves the gate fires
/// independent of blob size — not just for sub-interval blobs. Asserting on the
/// server's `StreamResponse` (rather than the buyer's error string) proves the
/// success path was never entered: an `ok: true` would have streamed bytes.
#[tokio::test(flavor = "multi_thread")]
async fn client_unknown_channel_is_rejected() -> anyhow::Result<()> {
    let payload = vec![0xABu8; 1_572_864]; // 1.5 MiB — would cross a chunk if served
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // Empty store: the channel is unknown to the node.
    let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
    let (target, _server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store, RATE_PER_MB, 0, 16).await?;

    // A bound client whose lane is not in the (empty) store: the binding
    // resolves a lane key, but no lane exists for it, so the serve is refused as
    // an unknown channel (#848). Drive the raw path so we read the server's first
    // reply directly.
    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let signer = PrivateKeySigner::random();
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x5678,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(
                !resp.body.ok,
                "unknown channel must be refused pre-serve, not served"
            );
            anyhow::ensure!(
                matches!(
                    resp_ext.error,
                    Some(decdn_protocol::client::StreamError::NotFound)
                ),
                "expected NotFound, got {:?}",
                resp_ext.error
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
            "decdn_serve_stream_rejected_unknown_lane_total 1"
        ),
        "unknown-lane refusal must bump its reason counter"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1516 pre-serve deposit gate: a known, owned channel whose deposit cannot
/// cover the first credit window is refused *before* the node signs `ok: true`.
/// Without the gate the node signs and streams a whole window (one voucher
/// accounting interval) before the first voucher's pool-solvency check can fire
/// (`PoolExhausted`) — a free interval per request, on every request. The blob
/// exceeds the window, so the gate is what stops the stream, not the blob
/// running out. The deposit sits one base unit under the window's cost at
/// `RATE_PER_MB`.
#[tokio::test(flavor = "multi_thread")]
async fn client_underfunded_channel_is_refused_pre_serve() -> anyhow::Result<()> {
    // 6 MiB — larger than one credit window (the ramp floor, one chunk).
    let payload = vec![0xABu8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let window_cost = min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB);
    let deposit = window_cost.saturating_sub(U256::from(1u64));

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Pool remaining sits one base unit under the floor's cost. A large
    // `credit_max` makes no difference here — the default (non-zero) ramp
    // divisor prices the gate at the floor, not the ceiling.
    let (target, _server_eth, server_ep, server_task, metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        deposit,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x1516,
    };
    // Raw, so the server's FIRST reply is read directly: an `ok: true` here would
    // mean bytes were already committed to the wire.
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(
                !resp.body.ok,
                "an underfunded channel must be refused pre-serve, not served"
            );
            anyhow::ensure!(
                matches!(
                    resp_ext.error,
                    Some(decdn_protocol::client::StreamError::NotFound)
                ),
                "expected the collapsed NotFound wire code, got {:?}",
                resp_ext.error
            );
        }
        other => anyhow::bail!("expected a pre-serve StreamResponse refusal, got {other:?}"),
    }

    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 1"
        ),
        "the refusal must be distinguishable server-side — the wire code is lossy"
    );
    // The acceptance criterion of #1516: zero bytes served. The channel never
    // advanced, so nothing was delivered and nothing was owed.
    let persisted = store.load_all()?;
    let state = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("the seeded channel must still be persisted"))?;
    anyhow::ensure!(
        state.last_bytes_delivered() == U256::ZERO && state.last_amount() == U256::ZERO,
        "a refused request must deliver zero bytes; got {} bytes / {} owed",
        state.last_bytes_delivered(),
        state.last_amount()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The `min(credit window, request span)` term of the #1516 gate. The gate
/// reserves what the node can actually front before the first voucher, which for
/// a sub-interval blob is the blob — not a whole chunk it will never
/// stream. Reserving a whole interval here would price this request far above
/// the ~3 the transfer really costs, and refuse a deposit that comfortably
/// covers it, turning a correctness fix into a regression for small blobs.
#[tokio::test(flavor = "multi_thread")]
async fn client_sub_interval_blob_serves_below_one_interval_cost() -> anyhow::Result<()> {
    let payload = vec![0x2Cu8; 262_144]; // 256 KiB — a fraction of one chunk
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(5u64); // < one interval's cost, > this blob's (~3)
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, store_dyn, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, signer, deposit);
    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x2C01,
        Duration::from_secs(10),
    )
    .await?;
    anyhow::ensure!(got == payload, "the sub-interval blob must transfer intact");
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 0"
        ),
        "a funded sub-interval request must not trip the deposit gate"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1669: with the ramp enabled (a non-zero `credit_ramp_divisor`), the gate
/// prices at `paid = 0` — the floor, one chunk — not the fully-ramped
/// `credit_max` ceiling. A deposit that covers exactly the floor's cost clears
/// the gate even though `credit_max` is configured far larger: unlike the
/// pre-ramp flat window, a big ceiling no longer means the node fronts that much
/// before the first voucher.
#[tokio::test(flavor = "multi_thread")]
async fn client_deposit_gate_reserves_only_the_floor_by_default() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024; // far wider than the one-interval floor
    let payload = vec![0x77u8; 8 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(HARNESS_FLOOR_COST),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    // Pool remaining exactly covers the one-chunk floor, not the 64 MiB ceiling.
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        U256::from(HARNESS_FLOOR_COST),
        CREDIT_MAX,
        2,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x1477,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), _resp_ext) => anyhow::ensure!(
            resp.body.ok,
            "a deposit covering the floor must clear the gate regardless of credit_max"
        ),
        other => anyhow::bail!("expected a pre-serve StreamResponse acceptance, got {other:?}"),
    }

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1669: `credit_ramp_divisor = 0` opens the full `credit_max` ceiling
/// immediately — the pre-ramp flat-window behavior, now opt-in — so the
/// pre-serve gate must reserve the WHOLE ceiling, not the floor. A deposit that
/// only covers one interval is refused against a wider ceiling, exactly how
/// enabling the flat window would silently re-open the hole #1516 closes if the
/// gate only ever priced at the floor.
#[tokio::test(flavor = "multi_thread")]
async fn client_deposit_gate_scales_with_credit_max_when_ramp_disabled() -> anyhow::Result<()> {
    const CREDIT_MAX: u64 = 8 * 1024 * 1024; // two chunks — wider than the floor
    let payload = vec![0x77u8; 12 * 1024 * 1024]; // larger than the ceiling, so the ceiling binds
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(HARNESS_FLOOR_COST),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    // Pool remaining covers only the first chunk of the 8 MiB ceiling.
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let (target, _server_eth, server_ep, server_task, metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        U256::from(HARNESS_FLOOR_COST),
        CREDIT_MAX,
        0,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x1477,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), _resp_ext) => anyhow::ensure!(
            !resp.body.ok,
            "a deposit covering one interval must not unlock the whole ceiling"
        ),
        other => anyhow::bail!("expected a pre-serve StreamResponse refusal, got {other:?}"),
    }
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 1"
        ),
        "the credit-window refusal must bump the deposit counter"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The #1516 gate reserves remaining HEADROOM, not the gross deposit. Both
/// deposit authorities — `LaneState::stage_voucher` off-chain and
/// `PaymentPool._redeemVoucher` on-chain — compare the *cumulative*
/// voucher amount, so a long-lived lane that has already claimed most of its
/// cap has almost nothing left to spend. Here the gross deposit (100) is ten
/// times the window's cost and would sail through a gross-deposit check, while
/// the real headroom (5) cannot cover it. The `last_*` fields are private
/// (#751), so the spent-down watermark is built via `hydrate`.
#[tokio::test(flavor = "multi_thread")]
async fn client_spent_down_channel_is_refused_pre_serve() -> anyhow::Result<()> {
    let payload = vec![0x5Du8; 1_572_864]; // 1.5 MiB — well under one credit window
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(100u64),
        0,
        U256::from(95u64),
        U256::from(9_961_472u64),
        Some([0x22; 65]),
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Gross deposit 100, but the pool has only 5 remaining after prior redeems —
    // the headroom the gate reserves against, ten times under the window cost.
    let (target, _server_eth, server_ep, server_task, metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        U256::from(5u64),
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x5D01,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), _resp_ext) => anyhow::ensure!(
            !resp.body.ok,
            "a spent-down channel must be refused on headroom, not waved through on gross deposit"
        ),
        other => anyhow::bail!("expected a pre-serve StreamResponse refusal, got {other:?}"),
    }
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 1"
        ),
        "the spent-down refusal must bump the deposit counter"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The resume arm of the #1516 gate's span (`byte_offset > 0`, `byte_len == 0`),
/// which is the shape `decdn fetch --output` sends when continuing a partial
/// download. The gate must price only the remaining tail, not the whole blob.
///
/// This is the arm with teeth. `decdn fetch` treats a `NotFound` on a resume as
/// evidence the partial file may belong to another blob, so it rewinds to
/// `byte_offset = 0` and re-downloads — and re-pays for — everything it already
/// had (`crates/cli/src/commands/fetch.rs`, `resume_may_be_stale`). A gate that
/// mispriced a resume as the whole blob would therefore not merely refuse: it
/// would silently double the user's bill on every interrupted large fetch.
///
/// Sized so the two readings diverge: a 1.4 MiB blob resumed at 1 MiB leaves a
/// ~0.4 MiB tail costing 4, while the whole blob would cost 14 and one window
/// 10. A deposit of 5 covers only the tail, so pricing anything but the tail
/// refuses.
#[tokio::test(flavor = "multi_thread")]
async fn client_resumed_range_is_priced_on_the_tail_not_the_whole_blob() -> anyhow::Result<()> {
    const TAIL_OFFSET: u64 = 1_048_576;
    let payload = vec![0x9Eu8; 1_468_006]; // 1.4 MiB
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(5u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Pool remaining 5 covers the ~0.4 MiB tail (cost 4) but not the whole blob.
    let (target, _server_eth, server_ep, server_task, metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        U256::from(5u64),
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: TAIL_OFFSET,
        byte_len: 0,
        timestamp_us: 0x9E01,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => anyhow::ensure!(
            resp.body.ok,
            "a resume funded for its tail must be served, not refused: {:?}",
            resp_ext.error
        ),
        other => anyhow::bail!("expected a signed StreamResponse, got {other:?}"),
    }
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 0"
        ),
        "the resume must not trip the deposit gate"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The gate's boundary: `headroom == ceiling` must PASS, because the check is
/// `<` and a channel funded to exactly the window's cost can pay for it.
///
/// Left uncovered by the four refusal/serve tests — each sits strictly on one
/// side of the boundary, so flipping `<` to `<=` passes all of them while
/// refusing every exactly-funded channel with a lossy `NotFound` the CLI renders
/// as a missing blob. A `decdn fetch --working-deposit-micro-usdc` funded to the
/// computed cost is exactly this case.
#[tokio::test(flavor = "multi_thread")]
async fn client_headroom_equal_to_the_ceiling_is_served() -> anyhow::Result<()> {
    // One voucher accounting interval's worth, well under the window.
    let payload = vec![0xB0u8; 1_572_864];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let window_cost = min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB);

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        window_cost,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Pool remaining exactly equals the floor's cost — the `>=` boundary.
    let (target, _server_eth, server_ep, server_task, metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        window_cost,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0xB001,
    };
    // Only the pre-serve verdict is asserted. The transfer may still stop at a
    // later voucher — the ceiling is priced in content bytes while vouchers bill
    // wire bytes — and that is the mid-stream ceiling's job, not the gate's.
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => anyhow::ensure!(
            resp.body.ok,
            "headroom exactly equal to the ceiling must be served: {:?}",
            resp_ext.error
        ),
        other => anyhow::bail!("expected a signed StreamResponse, got {other:?}"),
    }
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 0"
        ),
        "an exactly-funded channel must not trip the deposit gate"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A `PoolStateStore` that hydrates its seeded channels (so vouchers reach
/// the apply path) but fails every `record` — exercises the store-record
/// failure path in [`ClientHandler::commit_one_proof`].
#[derive(Debug)]
struct FailingRecordStore {
    inner: MemoryPoolStateStore,
}

impl PoolStateStore for FailingRecordStore {
    fn load_all(&self) -> Result<Vec<LaneState>, decdn_incentive::StoreError> {
        self.inner.load_all()
    }

    fn record(&self, _state: &LaneState) -> Result<(), decdn_incentive::StoreError> {
        Err(decdn_incentive::StoreError::Io(std::io::Error::other(
            "injected transient store failure",
        )))
    }

    fn forget(&self, pool_id: LaneKey) -> Result<(), decdn_incentive::StoreError> {
        self.inner.forget(pool_id)
    }

    fn get(&self, pool_id: LaneKey) -> Result<Option<LaneState>, decdn_incentive::StoreError> {
        self.inner.get(pool_id)
    }
}

/// A `record` failure is now a hard serve fault, not a clean in-band rejection:
/// the buffered lane store's `record` is expected to fail only on a poisoned
/// mutex (ADR 003 §Off-chain voucher state persistence), so `commit_one_proof`
/// treats it as a fault and aborts the stream rather than writing a `StreamError`
/// frame — the client sees a transport-level failure, and the store never
/// observes the advanced watermark.
#[tokio::test(flavor = "multi_thread")]
async fn client_store_record_failure_aborts_the_stream() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let inner = MemoryPoolStateStore::new();
    inner.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store: Arc<dyn PoolStateStore> = Arc::new(FailingRecordStore { inner });

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        Arc::clone(&store),
        RATE_PER_MB,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let ctx = channel_context(&client_ep, client_signer.clone(), deposit);

    anyhow::ensure!(
        stream_fetch(
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
        .is_err(),
        "a store record failure must not complete the fetch"
    );
    // The stored row never advanced past the seed — the record failure never
    // reached the store, so nothing was ever accepted. (The handler's in-memory
    // lane does advance before the record; the aborted stream is what stops it
    // mattering.)
    let persisted = store
        .get(lane_key(client_signer.address()))?
        .ok_or_else(|| anyhow::anyhow!("lane row missing"))?;
    anyhow::ensure!(
        persisted.last_bytes_delivered() == U256::ZERO,
        "a failed record must not advance the watermark, got {}",
        persisted.last_bytes_delivered()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A lane whose capability `expiry` is already in the past is refused in-band and
/// the stream finishes cleanly (no QUIC reset) — the client reads an actionable
/// reason instead of an opaque drop (#751). An expired grant surfaces as
/// `VoucherRejected { CapabilityExpired }`, distinct from a cap-exhausted
/// `SpendingCapExhausted` — the fix is a fresh capability, not a cap raise.
#[tokio::test(flavor = "multi_thread")]
async fn client_expired_channel_is_rejected_with_expired() -> anyhow::Result<()> {
    let payload = vec![0x5Au8; 4096];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    // Seed a channel whose on-chain expiry is already in the past (Unix second
    // `1`), so the serve-gate refuses the first voucher.
    let mut expired = LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );
    expired.expiry = 1;
    store.record(&expired)?;
    let store_dyn: Arc<dyn PoolStateStore> = store;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
    let ctx = channel_context(&client_ep, client_signer, deposit);

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
        err.to_string().contains("CapabilityExpired"),
        "error should surface the expired grant as CapabilityExpired: {err}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A channel store seeded with one channel owned by a fresh client signer.
/// Returns the store, that client signer, and the deposit.
fn seeded_store() -> anyhow::Result<(Arc<dyn PoolStateStore>, Arc<PrivateKeySigner>, U256)> {
    let signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    Ok((store, signer, deposit))
}

/// Spin up a server endpoint running a `ClientHandler` over `cache`/`store`,
/// returning the dialable target, the node's Ethereum signer (for `slash_sig`
/// verification), the server endpoint, and its accept-loop handle. `max_blob`
/// of `0` disables the size gate.
async fn spawn_handler_server(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
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
    store: Arc<dyn PoolStateStore>,
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
    let server_eth = operator_signer();
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

/// A [`decdn_node::pool_view::PoolView`] returning a fixed `remaining` (and a
/// benign, non-denied `owner`) for every pool. The floor-`M` pre-serve deposit
/// gate reads the pool's on-chain `remaining` through this view; the loopback
/// suite has no chain, so the deposit-gate tests wire this stub to drive the gate
/// with a known headroom.
#[derive(Debug)]
struct FixedRemainingPoolView {
    owner: Address,
    remaining: U256,
}

#[async_trait]
impl decdn_node::pool_view::PoolView for FixedRemainingPoolView {
    async fn status(&self, _pool_id: B256) -> Option<decdn_node::pool_view::PoolStatus> {
        Some(decdn_node::pool_view::PoolStatus {
            owner: self.owner,
            remaining: self.remaining,
            lifecycle: decdn_node::pool_view::Lifecycle::Open,
        })
    }
}

/// The harness's fixed one-chunk floor cost — `credit_window(chunk, paid = 0)`
/// collapses to one [`decdn_protocol::client::CHUNK_BYTES`] (1 MiB) regardless
/// of `credit_max` whenever `credit_ramp_divisor` is non-zero (ADR 003 §Credit
/// window, #1669).
///
/// Because `CHUNK_BYTES == BYTES_PER_MB`, one chunk prices to exactly the
/// quoted per-MB rate — so this is `RATE_PER_MB`, by the identity ADR 003
/// §Chunk Cadence rests on, rather than a number to keep in step by hand.
const HARNESS_FLOOR_COST: u64 = RATE_PER_MB;

/// `spawn_handler_server_with_metrics` with a fixed-`remaining` pool-view wired
/// and an explicit credit-window ceiling/ramp divisor — the setup the floor-`M`
/// pre-serve deposit-gate tests need so the gate reads a known pool headroom.
async fn spawn_handler_server_with_pool(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    remaining: U256,
    credit_max: u64,
    credit_ramp_divisor: u64,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<PrivateKeySigner>,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<Metrics>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let owner = operator_addr();
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        |deps| {
            deps.pool_view = Some(Arc::new(FixedRemainingPoolView { owner, remaining }));
            deps.credit_max = credit_max;
            deps.credit_ramp_divisor = credit_ramp_divisor;
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth, server_ep, server_task, metrics))
}

/// A [`PoolView`] whose `remaining` collapses from `high` to `low` the instant a
/// shared `drained` flag is set — the fixture for a pool that drains mid-flight
/// (other lanes redeeming its `remaining` down) so the mid-stream pool-solvency
/// re-check can be driven end-to-end. `owner` is fixed.
#[derive(Debug)]
struct DrainingPoolView {
    owner: Address,
    high: U256,
    low: U256,
    drained: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl decdn_node::pool_view::PoolView for DrainingPoolView {
    async fn status(&self, _pool_id: B256) -> Option<decdn_node::pool_view::PoolStatus> {
        let remaining = if self.drained.load(std::sync::atomic::Ordering::Relaxed) {
            self.low
        } else {
            self.high
        };
        Some(decdn_node::pool_view::PoolStatus {
            owner: self.owner,
            remaining,
            lifecycle: decdn_node::pool_view::Lifecycle::Open,
        })
    }
}

/// `spawn_handler_server_with_pool` wired with a [`DrainingPoolView`] instead of a
/// fixed one, so a test can drop the pool's `remaining` mid-stream by flipping the
/// shared `drained` flag.
async fn spawn_handler_server_with_draining_pool(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    high: U256,
    low: U256,
    drained: Arc<std::sync::atomic::AtomicBool>,
    recheck_interval: Duration,
) -> anyhow::Result<(EndpointAddr, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let owner = operator_addr();
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        |deps| {
            deps.pool_view = Some(Arc::new(DrainingPoolView {
                owner,
                high,
                low,
                drained,
            }));
            deps.pool_recheck_interval = Some(recheck_interval);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_ep, server_task))
}

/// Mid-stream `PoolExhausted`: a live stream stops IN-BAND at a voucher boundary
/// once its pool can no longer fund the pool's already-committed floor credit.
///
/// Two DISTINCT lanes on ONE pool — different signers, so their voucher
/// watermarks are independent (no shared-lane aggregate voucher): a "holder" (A)
/// that parks mid-delivery holding its live floor reservation, and a "driven"
/// stream (B) whose first voucher boundary the test crosses AFTER draining the
/// pool. When B pays interval 1 it releases its own floor, leaving A's floor as
/// the pool's committed credit; the drained `remaining` (`low = 10`) no longer
/// covers it (one `HARNESS_FLOOR_COST`, `M = 0`), so B's boundary re-check emits
/// a clean `PoolExhausted` and finishes the stream — not a QUIC reset. Per the
/// loopback close-code caveat we assert the received reason, never an app code.
///
/// The handler runs with a ZERO recheck interval so the mid-stream re-check
/// fires at every voucher boundary — this test isolates the drain→`PoolExhausted`
/// path itself; the wall-clock cadence gating is covered separately by
/// [`pool_recheck_gated_by_interval_lets_drained_stream_continue`] and
/// [`pool_recheck_fires_after_interval_on_drained_pool`].
#[tokio::test(flavor = "multi_thread")]
async fn mid_stream_pool_drain_stops_with_pool_exhausted() -> anyhow::Result<()> {
    // A is > one interval so it parks (never finishes) holding its reservation; B
    // is multi-interval so its first boundary is `!done` and the re-check runs.
    let payload_a = vec![0xA1u8; 6 * 1024 * 1024];
    let payload_b = vec![0xB2u8; 16 * 1024 * 1024];
    let (cache, hash_a, hash_b, _cache_tmp) = cache_with_two_blobs(&payload_a, &payload_b).await?;

    let signer_a = Arc::new(PrivateKeySigner::random());
    let signer_b = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    for signer in [&signer_a, &signer_b] {
        store.record(&LaneState::hydrate(
            pool_id(),
            signer.address(),
            operator_addr(),
            U256::from(10_000_000u64),
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        ))?;
    }
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // `high` (10_000) covers both lanes' admission floors; `low` covers neither,
    // so once A's floor is the pool's only committed credit
    // `remaining − M < HARNESS_FLOOR_COST` and B's re-check trips. M = 0 here.
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (target, server_ep, server_task) = spawn_handler_server_with_draining_pool(
        cache,
        store_dyn,
        U256::from(10_000u64),
        // Drained: strictly under ONE floor, so the mid-stream re-check cannot
        // cover even the single reservation still held. Derived from the floor
        // rather than hard-coded, so it tracks the payment quantum.
        U256::from(HARNESS_FLOOR_COST - 1),
        Arc::clone(&drained),
        Duration::ZERO,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let ext_a = binding_ext(&signer_a, client_node_id)?;
    let ext_b = binding_ext(&signer_b, client_node_id)?;

    // A: admit (reserves one floor), deliver its first interval, then leave it
    // PARKED reading a voucher we never send — it holds its live floor reservation
    // for the pool's whole `committed` while the test runs, well inside the 10s
    // voucher-read timeout. `_send_a` is kept so the stream stays open.
    let (_send_a, mut recv_a) = open_paid_stream(&conn, *hash_a.as_bytes(), Some(&ext_a)).await?;
    read_exact_chunks(&mut recv_a, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv_a).await?;

    // B: admit (reserves a second floor — `high` covers both), deliver its first
    // interval, park.
    let (mut send_b, mut recv_b) =
        open_paid_stream(&conn, *hash_b.as_bytes(), Some(&ext_b)).await?;
    read_exact_chunks(&mut recv_b, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv_b).await?;

    // Drain the pool: `remaining` now reads `low` on every subsequent `status()`.
    drained.store(true, std::sync::atomic::Ordering::Relaxed);

    // Pay B's first interval. The server commits it, releases B's own floor, then
    // runs the mid-stream re-check: A's still-held floor is the pool's committed
    // credit and the drained `remaining` cannot cover it, so B stops in-band.
    pay_cumulative(&mut send_b, &signer_b, HARNESS_INTERVAL_BYTES).await?;

    match tokio::time::timeout(Duration::from_secs(10), read_client_msg(&mut recv_b)).await?? {
        ClientMessage::StreamError(decdn_protocol::client::StreamError::VoucherRejected {
            reason,
            bundle,
        }) => {
            anyhow::ensure!(
                reason == VoucherRejectReason::PoolExhausted,
                "expected PoolExhausted, got {reason:?}"
            );
            anyhow::ensure!(
                bundle.is_none(),
                "PoolExhausted is not watermark-gated, so no bundle attaches"
            );
        }
        other => anyhow::bail!("expected VoucherRejected {{ PoolExhausted }}, got {other:?}"),
    }

    // The stream ended CLEANLY (in-band error then `finish`), not a QUIC reset:
    // the next read hits the FIN as a clean end-of-stream, never another frame.
    anyhow::ensure!(
        tokio::time::timeout(Duration::from_secs(5), read_client_msg(&mut recv_b))
            .await?
            .is_err(),
        "after PoolExhausted the server finishes the stream; no further frame is sent"
    );

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Live handles for the wall-clock recheck-cadence tests, produced by
/// [`setup_recheck_holder_and_driven`]. `_send_a`/`_recv_a` keep the holder lane
/// (A) open so it stays PARKED holding its floor reservation while a test drives B.
struct RecheckFixture {
    conn: Connection,
    client_ep: Endpoint,
    server_ep: Endpoint,
    server_task: tokio::task::JoinHandle<()>,
    _send_a: SendStream,
    _recv_a: RecvStream,
    send_b: SendStream,
    recv_b: RecvStream,
    signer_b: Arc<PrivateKeySigner>,
    drained: Arc<std::sync::atomic::AtomicBool>,
}

/// Shared fixture for the wall-clock recheck-cadence tests. Two DISTINCT lanes (A
/// the holder, B the driven) on one pool served by a [`DrainingPoolView`] with the
/// given `recheck_interval`. Admits both, delivers each its first interval, and
/// leaves A PARKED holding its floor reservation — so once the pool is drained and
/// B releases its own floor, A's floor is the pool's committed credit that the
/// drained `remaining` cannot cover (one `HARNESS_FLOOR_COST`,
/// `M = 0`). B is 16 MiB (4 intervals), so its first boundary is `!done`.
async fn setup_recheck_holder_and_driven(
    recheck_interval: Duration,
) -> anyhow::Result<RecheckFixture> {
    let payload_a = vec![0xA1u8; 6 * 1024 * 1024];
    let payload_b = vec![0xB2u8; 16 * 1024 * 1024];
    let (cache, hash_a, hash_b, _cache_tmp) = cache_with_two_blobs(&payload_a, &payload_b).await?;

    let signer_a = Arc::new(PrivateKeySigner::random());
    let signer_b = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    for signer in [&signer_a, &signer_b] {
        store.record(&LaneState::hydrate(
            pool_id(),
            signer.address(),
            operator_addr(),
            U256::from(10_000_000u64),
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        ))?;
    }
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (target, server_ep, server_task) = spawn_handler_server_with_draining_pool(
        cache,
        store_dyn,
        U256::from(10_000u64),
        // Drained: strictly under ONE floor, so the mid-stream re-check cannot
        // cover even the single reservation still held. Derived from the floor
        // rather than hard-coded, so it tracks the payment quantum.
        U256::from(HARNESS_FLOOR_COST - 1),
        Arc::clone(&drained),
        recheck_interval,
    )
    .await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let ext_a = binding_ext(&signer_a, client_node_id)?;
    let ext_b = binding_ext(&signer_b, client_node_id)?;

    // A: admit (reserves one floor), deliver its first interval, then park reading a
    // voucher we never send — it holds its live floor reservation for the whole test.
    let (send_a, mut recv_a) = open_paid_stream(&conn, *hash_a.as_bytes(), Some(&ext_a)).await?;
    read_exact_chunks(&mut recv_a, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv_a).await?;

    // B: admit (a second floor — `high` covers both), deliver its first interval, park.
    let (send_b, mut recv_b) = open_paid_stream(&conn, *hash_b.as_bytes(), Some(&ext_b)).await?;
    read_exact_chunks(&mut recv_b, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv_b).await?;

    Ok(RecheckFixture {
        conn,
        client_ep,
        server_ep,
        server_task,
        _send_a: send_a,
        _recv_a: recv_a,
        send_b,
        recv_b,
        signer_b,
        drained,
    })
}

/// Drive a paid stream to completion from the fixture's parked state and return the
/// total WIRE bytes received (bao content + proof, so `> ` the blob's content size).
///
/// A payment chunk spans one or a few delivery frames, so a per-frame voucher would sign
/// thousands of times; instead this pays one cumulative voucher per accumulated
/// voucher-interval (the efficient cadence the other loopback tests use). The
/// window ramp can leave a final sub-interval remainder the server parks on
/// awaiting payment — a read timeout is the park signal, so on a stall this settles
/// the outstanding remainder to unblock the server, then reads its `StreamEnd`.
async fn pay_and_read_to_end(
    send: &mut SendStream,
    recv: &mut RecvStream,
    signer: &PrivateKeySigner,
) -> anyhow::Result<u64> {
    // The fixture already delivered interval 1 and parked; pay it upfront so the
    // first loop read is not spent waiting out a park.
    let mut received = HARNESS_INTERVAL_BYTES;
    pay_cumulative(send, signer, received).await?;
    let mut paid = received;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), read_client_msg(recv)).await {
            Ok(msg) => match msg? {
                ClientMessage::ChunkData(chunk) => {
                    received = received.saturating_add(chunk.bytes().len() as u64);
                    if received.saturating_sub(paid) >= HARNESS_INTERVAL_BYTES {
                        pay_cumulative(send, signer, received).await?;
                        paid = received;
                    }
                }
                ClientMessage::StreamEnd => return Ok(received),
                other => {
                    anyhow::bail!("expected ChunkData or StreamEnd while draining, got {other:?}")
                }
            },
            // Read timed out: the server has parked. If a sub-interval remainder is
            // outstanding, settle it so `done` can fire; otherwise nothing we can pay
            // would advance the stream, so it is genuinely stuck.
            Err(_elapsed) => {
                anyhow::ensure!(
                    received > paid,
                    "stream stalled with nothing left to pay ({received} wire bytes received)"
                );
                pay_cumulative(send, signer, received).await?;
                paid = received;
            }
        }
    }
}

/// Wall-clock cadence, suppression half (ADR 003 §Pool solvency): with a re-check
/// interval far longer than the whole stream, the mid-stream re-check never comes
/// due, so a pool that drains mid-flight is NOT caught and B streams to completion.
/// A per-voucher-boundary check would trip `PoolExhausted` at B's first boundary
/// after the drain (that is exactly what
/// [`mid_stream_pool_drain_stops_with_pool_exhausted`] pins with a ZERO interval),
/// so B reaching `StreamEnd` across four boundaries proves the check fires at most
/// once per interval — here zero times.
#[tokio::test(flavor = "multi_thread")]
async fn pool_recheck_gated_by_interval_lets_drained_stream_continue() -> anyhow::Result<()> {
    // Longer than the whole (millisecond-scale) stream, so the re-check is never due.
    let mut fx = setup_recheck_holder_and_driven(Duration::from_secs(30)).await?;

    // Drain: every subsequent `status()` reports `low`, below A's committed floor.
    fx.drained.store(true, std::sync::atomic::Ordering::Relaxed);

    // Drive B to completion. With a per-boundary check the first payment after the
    // drain would trip `PoolExhausted`; the 30 s cadence gate suppresses every
    // check, so B receives the whole blob and ends with a clean `StreamEnd`.
    let received = pay_and_read_to_end(&mut fx.send_b, &mut fx.recv_b, &fx.signer_b).await?;
    anyhow::ensure!(
        received >= 16 * 1024 * 1024,
        "expected at least the full 16 MiB blob (wire = content + bao proof), received {received} bytes"
    );

    fx.conn.close(0u32.into(), b"done");
    shutdown([fx.server_task], [&fx.client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// Wall-clock cadence, firing half (ADR 003 §Pool solvency): once the re-check
/// interval has elapsed, the NEXT voucher boundary on a drained pool trips
/// `PoolExhausted`. We drain, wait past a short interval while B is parked (the
/// serve loop is blocked in the voucher read, so no check runs during the wait),
/// then pay B's first interval — by which point `last_pool_check.elapsed()` far
/// exceeds the interval, so the re-check runs and stops the stream in-band.
#[tokio::test(flavor = "multi_thread")]
async fn pool_recheck_fires_after_interval_on_drained_pool() -> anyhow::Result<()> {
    let interval = Duration::from_millis(300);
    let mut fx = setup_recheck_holder_and_driven(interval).await?;

    fx.drained.store(true, std::sync::atomic::Ordering::Relaxed);

    // Sleep past the interval so the next boundary is due. The wait only ever
    // lengthens `elapsed`, so the re-check is guaranteed due regardless of load.
    tokio::time::sleep(interval + Duration::from_millis(200)).await;

    // Pay B's first interval: the server commits it, releases B's own floor, then
    // runs the now-due re-check — A's still-held floor is the pool's committed
    // credit and the drained `remaining` cannot cover it, so B stops in-band.
    pay_cumulative(&mut fx.send_b, &fx.signer_b, HARNESS_INTERVAL_BYTES).await?;

    match tokio::time::timeout(Duration::from_secs(10), read_client_msg(&mut fx.recv_b)).await?? {
        ClientMessage::StreamError(decdn_protocol::client::StreamError::VoucherRejected {
            reason,
            bundle,
        }) => {
            anyhow::ensure!(
                reason == VoucherRejectReason::PoolExhausted,
                "expected PoolExhausted, got {reason:?}"
            );
            anyhow::ensure!(bundle.is_none(), "PoolExhausted attaches no bundle");
        }
        other => anyhow::bail!("expected VoucherRejected {{ PoolExhausted }}, got {other:?}"),
    }

    // Clean in-band stop, not a QUIC reset: the next read hits the FIN.
    anyhow::ensure!(
        tokio::time::timeout(Duration::from_secs(5), read_client_msg(&mut fx.recv_b))
            .await?
            .is_err(),
        "after PoolExhausted the server finishes the stream; no further frame is sent"
    );

    fx.conn.close(0u32.into(), b"done");
    shutdown([fx.server_task], [&fx.client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// Wall-clock cadence, solvent half (ADR 003 §Pool solvency): a re-check that
/// comes due on a still-SOLVENT pool does not stop the stream. With a short
/// interval and no drain, the re-check fires mid-stream, reads `remaining = high`
/// (which covers the committed floor), and B streams to a clean `StreamEnd`.
#[tokio::test(flavor = "multi_thread")]
async fn solvent_pool_stream_completes_across_recheck_interval() -> anyhow::Result<()> {
    let interval = Duration::from_millis(300);
    // No drain: `remaining` stays `high` for the whole stream.
    let mut fx = setup_recheck_holder_and_driven(interval).await?;

    // Cross an interval boundary in wall-clock time before the next payment, so at
    // least one re-check is guaranteed due while the pool is solvent.
    tokio::time::sleep(interval + Duration::from_millis(200)).await;

    // The re-check fires mid-stream, reads `remaining = high` (which covers the
    // committed floor), and lets B stream to a clean `StreamEnd`.
    let received = pay_and_read_to_end(&mut fx.send_b, &mut fx.recv_b, &fx.signer_b).await?;
    anyhow::ensure!(
        received >= 16 * 1024 * 1024,
        "expected at least the full 16 MiB blob (wire = content + bao proof), received {received} bytes"
    );

    fx.conn.close(0u32.into(), b"done");
    shutdown([fx.server_task], [&fx.client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// Seed a fresh in-memory pool store with `n` DISTINCT-signer lanes on the one
/// [`pool_id`], each generously funded on its own capability so the only budget
/// that ever bites is the pool-level floor-credit bound. Returns the store and the
/// signers, one per lane. The aggregate-bound tests drive each signer as its own
/// lane so the property under test is "total free floor data across ALL lanes is
/// bounded to `remaining − M`", never a per-lane limit.
fn store_with_distinct_lanes(
    n: usize,
) -> anyhow::Result<(Arc<MemoryPoolStateStore>, Vec<Arc<PrivateKeySigner>>)> {
    let store = Arc::new(MemoryPoolStateStore::new());
    let mut signers = Vec::with_capacity(n);
    for _ in 0..n {
        let signer = Arc::new(PrivateKeySigner::random());
        store.record(&fresh_lane(signer.address(), U256::from(10_000_000u64)))?;
        signers.push(signer);
    }
    Ok((store, signers))
}

/// `spawn_handler_server_with_pool` plus a wired in-memory [`PoolFloorLossStore`],
/// returned so a test can OBSERVE the durable per-pool `dead_charge` a dropped
/// `FloorReservation` folds. A guard's `Drop` writes `record_loss` AFTER it has
/// already advanced the in-memory `dead_charge` the admission gate reads, so once
/// the store reports a value the in-memory gate is guaranteed to see at least that
/// much committed credit. That ordering is the deterministic sync the
/// sequential-lane bound test needs between one lane's disconnect and the next
/// lane's admission — no sleeps, no clock faking. `remaining`/ramp are wired the
/// same way [`spawn_handler_server_with_pool`] does (M defaults to 0).
async fn spawn_pool_server_with_loss(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    remaining: U256,
) -> anyhow::Result<(
    EndpointAddr,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<decdn_incentive::MemoryPoolFloorLossStore>,
)> {
    // `10_000` bps: the per-signer sub-cap is at least the pool's whole headroom,
    // so the pool ceiling is the only floor bound these tests exercise.
    spawn_pool_server_with_signer_share(cache, store, remaining, 10_000).await
}

/// [`spawn_pool_server_with_loss`] with an explicit per-signer floor share in
/// basis points (ADR 003 §Pool solvency, per-signer floor isolation), so a test
/// can exercise the sub-cap rather than only the per-pool ceiling.
async fn spawn_pool_server_with_signer_share(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    remaining: U256,
    pool_floor_signer_share_bps: u64,
) -> anyhow::Result<(
    EndpointAddr,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<decdn_incentive::MemoryPoolFloorLossStore>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let owner = operator_addr();
    let loss = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
    let loss_dyn: Arc<dyn decdn_incentive::PoolFloorLossStore> = loss.clone();
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        |deps| {
            deps.pool_view = Some(Arc::new(FixedRemainingPoolView { owner, remaining }));
            deps.credit_max = decdn_common::config::DEFAULT_CREDIT_MAX;
            deps.credit_ramp_divisor = decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR;
            deps.floor_loss_store = Some(loss_dyn);
            deps.pool_floor_signer_share_bps = pool_floor_signer_share_bps;
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_ep, server_task, loss))
}

/// Fold a [`decdn_incentive::PoolFloorLossStore::load_losses`] dump into
/// [`pool_id`]'s total dead charge. Rows are keyed per `(pool, signer)` since the
/// per-signer floor sub-cap (ADR 003 §Pool solvency), and the pool total is their
/// sum — so a test that asserts a pool-level charge sums rather than picks a row,
/// and stays correct when more than one signer contributes.
fn pool_dead_charge_total(
    rows: Vec<(alloy::primitives::B256, alloy::primitives::Address, u128)>,
) -> u128 {
    rows.into_iter()
        .filter(|&(pool, _, _)| pool == pool_id())
        .map(|(_, _, micro)| micro)
        .fold(0u128, u128::saturating_add)
}

/// Poll `loss` until [`pool_id`]'s recorded dead charge in `µUSDC` reaches the target
/// `want`, or bail on overshoot / a 10 s stall. The dead-charge fold is a best-effort `Drop`
/// side effect persisted off-task via `spawn_blocking`, so a test cannot know the
/// exact instant it lands — it waits for the value, never for a fixed duration,
/// which keeps the assertion deterministic under parallel load.
async fn await_pool_dead_charge(
    loss: &decdn_incentive::MemoryPoolFloorLossStore,
    want: u128,
) -> anyhow::Result<()> {
    use decdn_incentive::PoolFloorLossStore as _;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let current = pool_dead_charge_total(loss.load_losses()?);
        if current == want {
            return Ok(());
        }
        anyhow::ensure!(
            current <= want,
            "pool dead_charge {current} overshot the expected {want}"
        );
        anyhow::ensure!(
            Instant::now() < deadline,
            "pool dead_charge stalled at {current}, expected {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Sign a voucher whose `signer` field names `lane_signer` (so the node resolves
/// the right lane) but whose ECDSA signature is produced by a DIFFERENT key, so it
/// recovers to the wrong address and is rejected `WrongSigner`. Drives the
/// first-iteration rejection path: a stream that took one free floor then sent an
/// unusable voucher.
async fn send_bad_signature_voucher(
    send: &mut SendStream,
    lane_signer: &PrivateKeySigner,
    bytes_delivered: u64,
) -> anyhow::Result<()> {
    let wrong = PrivateKeySigner::random();
    let amount = min_payment(bytes_delivered, RATE_PER_MB);
    let voucher = Voucher {
        pool_id: pool_id(),
        signer: lane_signer.address(),
        provider: operator_addr(),
        amount,
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&wrong, &payment_domain())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client_msg(
        send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)?),
    )
    .await
}

/// CORE security property (sequential): total FREE floor bytes a low-deposit pool
/// gives away is bounded to ~`remaining − M`, NOT `lanes × floor`, even as every
/// lane disconnects between admissions.
///
/// `remaining` covers exactly `affordable_floors` floors, `M = 0`, so the
/// pool can fund `140 / 40 = 3` floors of un-vouchered credit. Each round opens a
/// DISTINCT-signer lane on its OWN connection, reads exactly one floor, withholds
/// its voucher, and disconnects — so the stream's `FloorReservation` drops and
/// folds one floor (`40`) into the pool's DURABLE `dead_charge`. The test waits for
/// the loss store to confirm the fold before the next admission, so the counts are
/// exact. After three such lanes the pool has `dead_charge = 120`, and a FOURTH
/// distinct lane is refused `NotFound` at admission — proving the bound survives
/// each lane's disconnect. Without the durable `dead_charge`, a disconnected lane
/// would release its whole reservation and the pool would re-admit fresh lanes
/// forever (`lanes × floor` unbounded); the fourth refusal is exactly what that
/// non-durable design could never produce.
#[tokio::test(flavor = "multi_thread")]
async fn sequential_distinct_lanes_bounded_to_pool_deposit() -> anyhow::Result<()> {
    // > one interval so each lane parks after its floor rather than finishing.
    let payload = vec![0x5Au8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // remaining funds exactly THREE floors of dead credit; the fourth lane is refused.
    let affordable_floors = 3u128;
    // Slack strictly under one floor: enough that the gate is not sitting on an
    // exact boundary, but never enough to admit one more lane.
    let remaining = U256::from(u128::from(HARNESS_FLOOR_COST) * affordable_floors + 2);
    let (store, signers) = store_with_distinct_lanes(affordable_floors as usize + 1)?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    let (target, server_ep, server_task, loss) =
        spawn_pool_server_with_loss(cache, store_dyn, remaining).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());

    // Three lanes each take one free floor and vanish; `dead_charge` climbs 40 → 120.
    for (i, signer) in signers.iter().take(affordable_floors as usize).enumerate() {
        let conn = client_ep
            .connect(target.clone(), ALPN_CLIENT)
            .await
            .map_err(|e| anyhow::anyhow!("connect lane {i}: {e}"))?;
        let ext = binding_ext(signer, client_node_id)?;
        let (send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
        read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
        assert_parked_awaiting_voucher(&mut recv).await?;
        // Disconnect while withholding: the server's read errors, its serve returns,
        // and the `FloorReservation` folds one floor into the durable dead charge.
        drop(send);
        drop(recv);
        conn.close(0u32.into(), b"withhold");
        let expected = u128::from(HARNESS_FLOOR_COST) * (i as u128 + 1);
        await_pool_dead_charge(&loss, expected).await?;
    }

    // The fourth DISTINCT lane is refused: `dead_charge (120) + floor (40) > remaining
    // − M (140)`. A durable bound the three disconnects could not reset.
    let last = signers
        .get(affordable_floors as usize)
        .ok_or_else(|| anyhow::anyhow!("missing the fourth lane signer"))?;
    let conn = client_ep
        .connect(target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect refused lane: {e}"))?;
    let ext = binding_ext(last, client_node_id)?;
    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn, *hash.as_bytes(), Some(&ext)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "the fourth distinct lane must be refused once the pool's free-floor budget is spent"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "expected the collapsed NotFound wire code, got {:?}",
        refusal_ext.error
    );

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// CORE security property (per-signer isolation, #1841): one signer that opens and
/// abandons streams is locked out by its OWN share of the pool's free-floor budget,
/// while a co-tenant signer on the SAME pool is still served. The abandoning
/// signer's `dead_charge` is charged to its own entry, so it exhausts that signer's
/// share without reaching the co-tenant's.
///
/// `remaining = 4 floors + slack`, `M = 0`, share `2500` bps: a quarter of the
/// headroom is one floor, so each signer's sub-cap is one floor. Signer A withholds
/// one floor and disconnects, filling its own share; its SECOND attempt is refused
/// even though three floors of pool headroom remain untouched. Signer B, which has
/// spent nothing, is admitted from its own share at that same instant — which is
/// only possible if the accumulator carries a signer dimension.
#[tokio::test(flavor = "multi_thread")]
async fn one_signer_at_its_share_does_not_lock_out_a_co_tenant() -> anyhow::Result<()> {
    // > one interval so each lane parks after its floor rather than finishing.
    let payload = vec![0x7Cu8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // Four floors of headroom at a quarter share => one floor per signer, which is
    // exactly the one-credit-window clamp, so the share is what binds either way.
    let remaining = U256::from(u128::from(HARNESS_FLOOR_COST) * 4 + 2);
    let (store, signers) = store_with_distinct_lanes(2)?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let (target, server_ep, server_task, loss) =
        spawn_pool_server_with_signer_share(cache, store_dyn, remaining, 2_500).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let signer_a = signers
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing signer A"))?;
    let signer_b = signers
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing signer B"))?;

    // Signer A takes its one free floor and vanishes, filling its own share.
    let conn = client_ep
        .connect(target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect A: {e}"))?;
    let ext = binding_ext(signer_a, client_node_id)?;
    let (send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;
    drop(send);
    drop(recv);
    conn.close(0u32.into(), b"withhold");
    await_pool_dead_charge(&loss, u128::from(HARNESS_FLOOR_COST)).await?;

    // A's SECOND attempt is refused: its share is spent. Three floors of pool
    // headroom are still free, so nothing but the sub-cap can be refusing it.
    let conn_a2 = client_ep
        .connect(target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect A again: {e}"))?;
    let ext_a2 = binding_ext(signer_a, client_node_id)?;
    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn_a2, *hash.as_bytes(), Some(&ext_a2)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "a signer that filled its own share must be refused while the pool can still pay"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "the sub-cap refusal collapses to NotFound on the wire, got {:?}",
        refusal_ext.error
    );
    conn_a2.close(0u32.into(), b"capped");

    // Signer B, a co-tenant on the SAME pool, is served from its own share.
    let conn_b = client_ep
        .connect(target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect B: {e}"))?;
    let ext_b = binding_ext(signer_b, client_node_id)?;
    let (send_b, mut recv_b) = open_paid_stream(&conn_b, *hash.as_bytes(), Some(&ext_b)).await?;
    read_exact_chunks(&mut recv_b, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv_b).await?;
    drop(send_b);
    drop(recv_b);
    conn_b.close(0u32.into(), b"done");

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// CORE security property (concurrent): with many DISTINCT-signer streams held open
/// at once, admissions stop as soon as `Σ live floors` would exceed `remaining −
/// M`. The concurrent twin of the sequential test — here the bound is enforced by
/// the LIVE reservation, not the durable dead charge.
///
/// `remaining = 140`, `M = 0`, floor `40`: three lanes fit (`3 × 40 = 120 ≤ 140`),
/// a fourth does not (`160 > 140`). Each lane is opened on its own bi-stream,
/// reads one floor, and PARKS holding its live reservation; the streams stay open
/// (their send/recv halves are kept alive) so all three reservations are held
/// concurrently. With three live floors held, a fourth distinct lane is refused
/// `NotFound`. Non-vacuous: a fourth lane on its own would fit (`40 ≤ 140`); it is
/// refused only because the three concurrent reservations already committed the
/// budget.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_distinct_lanes_bounded_to_pool_deposit() -> anyhow::Result<()> {
    let payload = vec![0x6Bu8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let affordable_floors = 3usize;
    // Slack strictly under one floor — see the sequential twin.
    let remaining = U256::from(u128::from(HARNESS_FLOOR_COST) * affordable_floors as u128 + 2);
    let (store, signers) = store_with_distinct_lanes(affordable_floors + 1)?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        remaining,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    // Admit and PARK three concurrent distinct lanes; keep every stream alive so its
    // live floor reservation is held for the duration.
    let mut held = Vec::new();
    for (i, signer) in signers.iter().take(affordable_floors).enumerate() {
        let ext = binding_ext(signer, client_node_id)?;
        let (send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
        read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
        assert_parked_awaiting_voucher(&mut recv).await?;
        held.push((i, send, recv));
    }

    // Fourth concurrent distinct lane: refused — `Σ live floors (120) + floor (40) >
    // remaining − M (140)`.
    let fourth = signers
        .get(affordable_floors)
        .ok_or_else(|| anyhow::anyhow!("missing the fourth lane signer"))?;
    let ext = binding_ext(fourth, client_node_id)?;
    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn, *hash.as_bytes(), Some(&ext)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "the fourth concurrent lane must be refused while three live floors are held"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "expected the collapsed NotFound wire code, got {:?}",
        refusal_ext.error
    );

    drop(held);
    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A topped-up (high-`remaining`) pool serves many concurrent distinct lanes with
/// NO refusal — the Netflix case is unaffected by the floor-credit bound. The
/// control for the two bound tests above: the same concurrent distinct-lane setup,
/// but a `remaining` so large the bound never bites, so every lane is admitted and
/// served. `open_paid_stream` bails on any non-`ok` `StreamResponse`, so all six
/// opens succeeding IS the "no refusal" assertion; reading a floor from each proves
/// each was really served, not merely admitted.
#[tokio::test(flavor = "multi_thread")]
async fn topped_up_pool_serves_many_lanes() -> anyhow::Result<()> {
    let payload = vec![0x7Cu8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // Six lanes × one floor = 240 µUSDC of live credit, dwarfed by `remaining`.
    let lanes = 6usize;
    let remaining = U256::from(10_000_000u64);
    let (store, signers) = store_with_distinct_lanes(lanes)?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    let (target, _server_eth, server_ep, server_task, _metrics) = spawn_handler_server_with_pool(
        cache,
        store_dyn,
        remaining,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
    )
    .await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let mut held = Vec::new();
    for (i, signer) in signers.iter().enumerate() {
        let ext = binding_ext(signer, client_node_id)?;
        // `open_paid_stream` bails if the server refuses (`!ok`), so reaching the
        // read proves lane `i` was admitted against the topped-up budget.
        let (send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext))
            .await
            .map_err(|e| anyhow::anyhow!("lane {i} must be admitted on a topped-up pool: {e}"))?;
        read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
        held.push((send, recv));
    }
    anyhow::ensure!(
        held.len() == lanes,
        "every one of the {lanes} distinct lanes must be served on a topped-up pool, served {}",
        held.len()
    );

    drop(held);
    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Folded-in coverage: a stream that takes one free floor then DISCONNECTS folds
/// exactly one floor into the pool's durable `dead_charge`. The sequential bound
/// test asserts this indirectly (via a later refusal); here it is asserted
/// DIRECTLY against the loss store — `dead_charge == HARNESS_FLOOR_COST` after one
/// withholding lane — pinning the fold AMOUNT, not just its existence.
///
/// This runs on the HIT path (`deliver`). The MISS path (`serve_via_backend_origin`
/// / `serve_via_window_pull_through` → `serve_leg`) folds `dead_charge` through the
/// SAME `FloorReservation::note_unpaid` / `Drop` hooks — a real miss-path loopback
/// assertion would need an origin/pull-through server fixture this test module does
/// not yet stand up (see the task report), but the accounting it exercises is
/// identical.
#[tokio::test(flavor = "multi_thread")]
async fn withholding_lane_records_one_floor_of_dead_charge() -> anyhow::Result<()> {
    let payload = vec![0x8Du8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    // remaining comfortably admits one floor; the point is the fold amount, not a refusal.
    let remaining = U256::from(1_000u64);
    let (store, signers) = store_with_distinct_lanes(1)?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let signer = signers
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing lane signer"))?;

    let (target, server_ep, server_task, loss) =
        spawn_pool_server_with_loss(cache, store_dyn, remaining).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let ext = binding_ext(signer, client_node_id)?;
    let (send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;
    drop(send);
    drop(recv);
    conn.close(0u32.into(), b"withhold");

    // Exactly one floor folded — not zero (the interval WAS delivered) and not more
    // (the fold is capped at the reserved floor).
    await_pool_dead_charge(&loss, u128::from(HARNESS_FLOOR_COST)).await?;

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Folded-in coverage (guards commit `d441c025`): a stream that takes one free
/// floor then sends a REJECTED first voucher still folds one floor into
/// `dead_charge`. The rejection returns from the recoup block on the stream's FIRST
/// iteration, before the end-of-iteration reconcile — so the fold relies on the
/// pre-recoup `note_unpaid` that `d441c025` added. A regression that dropped that
/// note would leave `dead_charge == 0` here, letting "connect, take one free
/// interval, send garbage, vanish" escape the accounting.
#[tokio::test(flavor = "multi_thread")]
async fn rejected_first_voucher_still_records_dead_charge() -> anyhow::Result<()> {
    let payload = vec![0x9Eu8; 6 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let remaining = U256::from(1_000u64);
    let (store, signers) = store_with_distinct_lanes(1)?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let signer = signers
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing lane signer"))?;

    let (target, server_ep, server_task, loss) =
        spawn_pool_server_with_loss(cache, store_dyn, remaining).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    let ext = binding_ext(signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    assert_parked_awaiting_voucher(&mut recv).await?;

    // First voucher's signature recovers to the wrong address → WrongSigner; the
    // server rejects it in-band and finishes the serve, dropping the reservation on
    // its first iteration.
    send_bad_signature_voucher(&mut send, signer, HARNESS_INTERVAL_BYTES).await?;

    // The client receives the in-band rejection...
    match tokio::time::timeout(Duration::from_secs(10), read_client_msg(&mut recv)).await?? {
        ClientMessage::StreamError(decdn_protocol::client::StreamError::VoucherRejected {
            reason,
            ..
        }) => anyhow::ensure!(
            reason == VoucherRejectReason::WrongSigner,
            "expected WrongSigner, got {reason:?}"
        ),
        other => anyhow::bail!("expected VoucherRejected {{ WrongSigner }}, got {other:?}"),
    }

    // ...and the delivered-but-unpaid floor is folded despite the first-iteration exit.
    await_pool_dead_charge(&loss, u128::from(HARNESS_FLOOR_COST)).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
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
) -> anyhow::Result<(ClientMessage, decdn_protocol::StreamResponseExt)> {
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
    let (msg, rest) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    // Only a StreamResponse carries a trailing extension; anything else leaves an
    // empty remainder, which reads back as the default.
    let ext = decdn_protocol::parse_stream_response_ext(rest)
        .map_err(|e| anyhow::anyhow!("decode response ext: {e}"))?;
    Ok((msg, ext))
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
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = channel_context(&client_ep, signer, deposit);
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
        progress.advanced().is_none(),
        "no voucher should be paid when the buyer rejects up front: {:?}",
        progress.advanced()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = channel_context(&client_ep, signer, deposit);
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
        progress.advanced().is_none(),
        "no voucher may be paid when the buyer refuses the rate up front: {:?}",
        progress.advanced()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Delivery-progress hook (#1118): `stream_fetch_tracked_with_progress` invokes
/// the callback as bytes arrive with `(wire_bytes_received, wire_bytes_expected)`.
/// The expected total is fixed once the signed `StreamResponse` arrives, received
/// bytes advance monotonically, and the final observation reaches the full
/// promised wire length — the contract `decdn fetch`'s progress bar relies on.
#[tokio::test(flavor = "multi_thread")]
async fn progress_callback_reports_monotonic_delivery() -> anyhow::Result<()> {
    // Multi-frame payload (256 KiB) so the callback fires repeatedly
    // and the monotonic-advance assertion has intermediate points to check.
    let payload = vec![0xABu8; 256 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
/// `owner` is the pool's funder as the serve gate reads it from `getPool.owner`
/// (via a stub pool-view); the origin-blacklist gate refuses a pool whose owner
/// is on the deny-set. Pool `remaining` is unbounded so only the deny-set — not
/// the floor-`M` gate — can refuse.
async fn spawn_handler_server_with_deny(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    deny: Arc<decdn_node::content_deny::ContentDenylist>,
    owner: Address,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<PrivateKeySigner>,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<Metrics>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
            deps.content_deny = deny;
            deps.pool_view = Some(Arc::new(FixedRemainingPoolView {
                owner,
                remaining: U256::MAX,
            }));
        },
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
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
/// flowing. The blob is sized to many chunks so boundaries remain
/// after the flip.
#[tokio::test(flavor = "multi_thread")]
async fn takedown_mid_stream_terminates_the_delivery() -> anyhow::Result<()> {
    // 4 voucher accounting intervals at the fixed `CHUNK_BYTES`.
    let payload = vec![0x5Au8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let deny_cache = cache.clone();
    let (target, server_eth, server_ep, server_task, metrics) =
        spawn_handler_server_with_metrics(cache, Arc::clone(&store), RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, signer, deposit);
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

    // Wait for the first accepted PROOF — the store is written on every acceptance,
    // so a non-zero `owed` means delivery is under way.
    //
    // `owed`, not `last_amount`: under `PayWord` the signed cumulative does not
    // move per chunk. A reveal advances the lane's claim without a signature, so
    // `last_amount` would sit at the opening voucher's value until the close —
    // by which point the whole blob is delivered and there is no mid-stream left
    // to interrupt.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !store
        .load_all()?
        .iter()
        .any(|state| state.owed() > U256::ZERO)
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
        spawn_handler_server_with_deny(cache, store, deny, funder).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = channel_context(&client_ep, signer, deposit);
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00ba_d001,
    };
    let res = raw_request(&client_ep, target, &req, Some(&ext)).await;
    anyhow::ensure!(
        res.is_err(),
        "a binding recovering a different address must reset the stream, got {res:?}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00ba_d002,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(
                !resp.body.ok,
                "expected ok:false for an unauthorized binding"
            );
            anyhow::ensure!(
                matches!(
                    resp_ext.error,
                    Some(decdn_protocol::client::StreamError::NotFound)
                ),
                "expected NotFound, got {:?}",
                resp_ext.error
            );
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }

    // Pool model: a valid binding for an address with no lane on this pool
    // resolves to no lane, so the refusal is the unknown-lane arm — the
    // owner-mismatch gate is subsumed into lane resolution (a wrong signer simply
    // owns no lane). Wire-indistinguishable from a cache miss (all `NotFound`), so
    // only the reason counter proves which arm ran (#876).
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_unknown_lane_total 1"
        ),
        "a bound-but-laneless request must bump the unknown-lane counter"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// What [`delegate_signer_store`] hands back: the seeded store, the funder
/// signer, and the delegate voucher signer.
type DelegateSignerFixture = (
    Arc<dyn PoolStateStore>,
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        delegate.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
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
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e01,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(
                resp.body.ok,
                "the pinned voucher signer must be authorized, got {:?}",
                resp_ext.error
            );
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e02,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(!resp.body.ok, "expected ok:false for the funder binding");
            anyhow::ensure!(
                matches!(
                    resp_ext.error,
                    Some(decdn_protocol::client::StreamError::NotFound)
                ),
                "expected NotFound, got {:?}",
                resp_ext.error
            );
        }
        other => anyhow::bail!("expected a StreamResponse, got {other:?}"),
    }
    // Pool model: the funder holds no lane on this delegated pool (the lane is
    // keyed by the delegate signer), so its binding resolves to no lane and the
    // refusal is the unknown-lane arm.
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_unknown_lane_total 1"
        ),
        "the funder binding must bump the unknown-lane counter"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
        spawn_handler_server_with_deny(cache, store, deny, funder.address()).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;

    let ext = binding_ext(&delegate, client_node_id)?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00de_1e03,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(
                !resp.body.ok,
                "a blacklisted funder must be refused behind a clean delegate"
            );
            anyhow::ensure!(
                matches!(
                    resp_ext.error,
                    Some(decdn_protocol::client::StreamError::OriginBlacklisted)
                ),
                "expected OriginBlacklisted, got {:?}",
                resp_ext.error
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let (store, _funder, delegate) = delegate_signer_store()?;
    let (target, server_eth, server_ep, server_task, _metrics) =
        spawn_handler_server_with_metrics(cache, Arc::clone(&store), RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    // The requester signs with the DELEGATE — the funder's key never appears.
    let ctx = channel_context(&client_ep, Arc::clone(&delegate), U256::from(10_000_000u64));
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
    // ADR 038: the metered quantity is bao wire bytes, not payload bytes.
    let wire = support::bao_wire_len_whole(payload.len() as u64);
    anyhow::ensure!(
        state.last_bytes_delivered() == U256::from(wire),
        "the persisted byte count must cover the whole blob: {} (expected {wire})",
        state.last_bytes_delivered()
    );
    anyhow::ensure!(
        state.signer == delegate.address() && state.provider == operator_addr(),
        "the delegate is the lane signer across a completed delivery"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    // 4 voucher accounting intervals at the fixed `CHUNK_BYTES`, so
    // plenty of boundaries remain after the deny-set flip.
    let payload = vec![0x6Bu8; 16 * 1024 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, funder, delegate) = delegate_signer_store()?;
    // Starts empty — nothing is denied at open time, so the request is admitted
    // and the cut-off can only come from the mid-stream re-check.
    let deny = Arc::new(decdn_node::content_deny::ContentDenylist::empty());
    let (target, server_eth, server_ep, server_task, metrics) = spawn_handler_server_with_deny(
        cache,
        Arc::clone(&store),
        Arc::clone(&deny),
        funder.address(),
    )
    .await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, Arc::clone(&delegate), U256::from(10_000_000u64));
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
        .any(|state| state.owed() > U256::ZERO)
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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

/// Concurrent pulls on ONE channel coordinate through a shared [`PoolLedger`]:
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
    let ctx = channel_context(&client_ep, Arc::clone(&signer), deposit);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
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

    // The lane advanced monotonically: the cumulative bytes cover BOTH payloads
    // and a non-zero amount was paid. Both pulls completed, so every voucher they
    // sent optimistically has cleared — `committed` carries the full total.
    let final_cum = ledger.committed();
    anyhow::ensure!(
        final_cum.amount > U256::ZERO,
        "ledger amount: {} (expected a paid delivery)",
        final_cum.amount
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A node without the blob answers `ok: false` with `NotFound`; the requester
/// surfaces the refusal without paying.
#[tokio::test(flavor = "multi_thread")]
async fn client_not_found_is_refused() -> anyhow::Result<()> {
    let (cache, _cache_tmp) = empty_cache().await?;

    let client_signer = Arc::new(PrivateKeySigner::random());
    let deposit = U256::from(10_000_000u64);
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        client_signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
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
    let ctx = channel_context(&client_ep, client_signer, deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// **#327 / #527 idempotency guard.** A re-observed `ChannelOpened` (which the
/// settlement watcher *will* replay after any RPC-error resubscription) must
/// NOT reset an already-advanced voucher watermark via `register_lane` — doing
/// so would reopen the replay window. Here the lane is already known with an
/// advanced watermark (hydrated from the store at construction); a fresh
/// `register_lane` for the same [`LaneKey`] must be a no-op.
#[tokio::test]
async fn register_lane_is_idempotent_and_preserves_watermark() -> anyhow::Result<()> {
    let (cache, _tmp) = empty_cache().await?;
    let store = Arc::new(MemoryPoolStateStore::new());
    let client = PrivateKeySigner::random().address();

    // Pre-seed an advanced watermark, as if vouchers had been accepted. The
    // `last_*` fields are private (#751), so build the watermark via `hydrate`.
    let advanced = LaneState::hydrate(
        pool_id(),
        client,
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::from(4_321u64),
        U256::from(2_048u64),
        Some([0x11; 65]),
        decdn_incentive::LaneChain::NONE,
    );
    store.record(&advanced)?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let server_eth = operator_signer();
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    let handler = build_handler(
        fresh_key().public(),
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
    )?;

    // A re-observed registration arrives as a fresh (zero-watermark) lane.
    handler
        .register_lane(fresh_lane(client, U256::from(10_000_000u64)))
        .await?;

    let after = store
        .get(lane_key(client))?
        .ok_or_else(|| anyhow::anyhow!("lane vanished"))?;
    anyhow::ensure!(
        after.last_bytes_delivered() == U256::from(2_048u64),
        "re-observed registration reset the watermark bytes to {} (reopened #527 replay window)",
        after.last_bytes_delivered()
    );
    anyhow::ensure!(after.last_amount() == U256::from(4_321u64));
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
    let server_eth = operator_signer();
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
        pool_id: pool_id().into(),
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
    shutdown([], [&c1]).await?;

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
    shutdown([], [&c2]).await?;

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
    shutdown([], [&c3]).await?;

    shutdown([server_task], [&server_ep]).await?;
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
    let server_eth = operator_signer();
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
        pool_id: pool_id().into(),
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
    shutdown([], [&c1]).await?;

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
    shutdown([], [&c2]).await?;

    shutdown([server_task], [&server_ep]).await?;
    Ok(())
}

/// Build a pull-through-armed handler over a `CountingOrigin` with an ADR 011
/// origin deny-set wired in, and return the dialable target plus the hit
/// counter. Shared by the two blacklist-subject tests below, which differ only
/// in which address is on the deny-set.
async fn spawn_counting_pull_server_with_deny(
    store: Arc<dyn PoolStateStore>,
    denied: &[alloy::primitives::Address],
    remaining: U256,
    owner: Address,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<std::sync::atomic::AtomicUsize>,
    Endpoint,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
    Arc<Metrics>,
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
    let server_eth = operator_signer();
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
            deps.pool_view = Some(Arc::new(FixedRemainingPoolView { owner, remaining }));
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, hits, server_ep, server_task, cache_tmp, metrics))
}

/// [`spawn_counting_pull_server_with_deny`] with an empty deny-set. `remaining`
/// is the pool headroom the floor-`M` pull-path gate reads through the stub
/// pool-view; `owner` is a benign, non-denied funder.
async fn spawn_counting_pull_server(
    store: Arc<dyn PoolStateStore>,
    remaining: U256,
    owner: Address,
) -> anyhow::Result<(
    EndpointAddr,
    Arc<std::sync::atomic::AtomicUsize>,
    Endpoint,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
    Arc<Metrics>,
)> {
    spawn_counting_pull_server_with_deny(store, &[], remaining, owner).await
}

/// #1519, the headline case: an underfunded channel must not make the node spend.
///
/// Every cache-miss fill tier is gated on channel OWNERSHIP (`pull_authorized`)
/// and none was gated on solvency, so before the pre-spend floor a dust-deposit
/// channel could name absent hashes, make the node pay its paid upstream for each,
/// and be refused afterwards by the serve-path gate. The attacker gained nothing —
/// #1516 closed the free-egress half — but the operator still paid.
///
/// `hits == 0` IS the acceptance criterion: the origin was never contacted. The
/// second counter assertion is what makes the test specific rather than merely
/// green — a refusal that came from the fill missing (rather than from the floor)
/// would bump `cache_miss` instead, and `hits == 0` alone cannot tell them apart.
#[tokio::test(flavor = "multi_thread")]
async fn underfunded_channel_never_reaches_the_paid_pull() -> anyhow::Result<()> {
    let window_cost = min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB);
    let deposit = window_cost.saturating_sub(U256::from(1u64));
    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Pool remaining sits one base unit under the window's cost.
    let (target, hits, server_ep, server_task, _cache_tmp, metrics) =
        spawn_counting_pull_server(store_dyn, deposit, signer.address()).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    // Bound, so `pull_authorized` would have said yes and the pull WOULD have run.
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: [0x19u8; 32], // never cached — a miss that would trigger the pull
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x1519,
    };
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), resp_ext) => {
            anyhow::ensure!(!resp.body.ok, "an underfunded miss must be refused");
            anyhow::ensure!(
                matches!(
                    resp_ext.error,
                    Some(decdn_protocol::client::StreamError::NotFound)
                ),
                "expected the collapsed NotFound wire code, got {:?}",
                resp_ext.error
            );
        }
        other => anyhow::bail!("expected a signed refusal, got {other:?}"),
    }

    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "the origin must never be contacted for a channel that cannot pay: {} hits",
        hits.load(std::sync::atomic::Ordering::SeqCst)
    );
    let text = metrics.encode()?;
    anyhow::ensure!(
        metric_line_present(
            &text,
            "decdn_serve_stream_rejected_insufficient_deposit_total 1"
        ),
        "the refusal must be attributed to the deposit floor"
    );
    anyhow::ensure!(
        metric_line_present(&text, "decdn_serve_stream_rejected_cache_miss_total 0"),
        "the refusal must come from the floor, not from the fill missing"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The boundary control for #1519, and the reason the floor cannot be quietly
/// tightened. A channel funded to EXACTLY one credit window clears the floor
/// (the check is `<`) and must still reach the pull.
///
/// Without this, raising the floor — or flipping `<` to `<=` — would break
/// nothing: `underfunded_channel_never_reaches_the_paid_pull` above only pins
/// that an *under*-funded channel is stopped. A floor that also stopped funded
/// channels would turn a cold fetch into a permanent refusal on every node.
#[tokio::test(flavor = "multi_thread")]
async fn funded_channel_still_reaches_the_paid_pull() -> anyhow::Result<()> {
    let window_cost = min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB);
    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        window_cost,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Pool remaining exactly equals the window's cost — the funded boundary.
    let (target, hits, server_ep, server_task, _cache_tmp, metrics) =
        spawn_counting_pull_server(store_dyn, window_cost, signer.address()).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: [0x1Au8; 32],
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x151A,
    };
    // `CountingOrigin` has nothing to serve, so the request still ends in a
    // refusal — but a `cache_miss` one, AFTER the origin was asked. Asserted rather
    // than discarded: `let _ =` would stay green on a malformed frame or an
    // unexpected `ok: true`.
    match raw_request(&client_ep, target, &req, Some(&ext)).await? {
        (ClientMessage::StreamResponse(resp), _resp_ext) => anyhow::ensure!(
            !resp.body.ok,
            "CountingOrigin holds nothing, so this must still end in a refusal"
        ),
        other => anyhow::bail!("expected a signed StreamResponse, got {other:?}"),
    }

    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "a channel funded to exactly the floor must still reach the pull: {} hits",
        hits.load(std::sync::atomic::Ordering::SeqCst)
    );
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 0"
        ),
        "an exactly-funded channel must not trip the deposit floor"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The `last_amount` term of the #1519 floor, and the case that actually happens
/// in production. The two tests above use a fresh channel, so
/// `deposit.saturating_sub(last_amount)` is never exercised — a mutant that reads
/// `let headroom = deposit;` passes the entire suite.
///
/// A *dust* channel is the adversarial shape; a *spent-out* channel is the common
/// one. Here the gross deposit is 10 USDC — a million times the floor — while the
/// remaining headroom is 5, one below it. Gating on the gross deposit would let
/// this channel drive an origin pull for every absent hash it names, forever,
/// which is the same drain #1519 closes against a dust channel.
#[tokio::test(flavor = "multi_thread")]
async fn spent_out_channel_never_reaches_the_paid_pull() -> anyhow::Result<()> {
    let client = PrivateKeySigner::random();
    let store = Arc::new(MemoryPoolStateStore::new());
    // `last_*` are private (#751), so the spent-down watermark comes from `hydrate`.
    store.record(&LaneState::hydrate(
        pool_id(),
        client.address(),
        operator_addr(),
        U256::from(10_000_000u64),
        0,
        U256::from(9_999_995u64),
        U256::from(1_048_575_488u64),
        Some([0x33; 65]),
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // Gross deposit 10 USDC, but only 5 remaining after prior redeems — well
    // below the window cost.
    let (target, hits, server_ep, server_task, _cache_tmp, metrics) =
        spawn_counting_pull_server(store_dyn, U256::from(5u64), client.address()).await?;

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&Arc::new(client), client_node_id)?;
    let req = StreamRequest {
        hash: [0x33u8; 32],
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x3301,
    };
    let _ = raw_request(&client_ep, target, &req, Some(&ext)).await?;

    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "a spent-out channel must not reach the origin despite a large gross deposit: {} hits",
        hits.load(std::sync::atomic::Ordering::SeqCst)
    );
    anyhow::ensure!(
        metric_line_present(
            &metrics.encode()?,
            "decdn_serve_stream_rejected_insufficient_deposit_total 1"
        ),
        "the refusal must be attributed to the deposit floor, not to the fill missing"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1518's invariant, which nothing asserted until now: one request clamps the
/// rate exactly once.
///
/// `clamped_rate()` bumps `rate_bounds_clamped` and warns, so a path that priced a
/// request and then let `respond_error` price it again double-counted a single
/// request. The fix made the rate a required argument of `respond_error`, which
/// stops the *implicit* recomputation — but a grep is what holds "exactly one
/// production call site", and greps do not run in CI. This does.
///
/// Driven through the #1519 floor specifically, because that is the path the
/// collapse was performed for: it prices the request and then falls through into
/// the fill ladder, whose miss arms also refuse.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_request_clamps_the_rate_exactly_once() -> anyhow::Result<()> {
    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id(),
        signer.address(),
        operator_addr(),
        U256::from(1u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();
    // An on-chain floor above the advertised rate, so every `clamped_rate()` call
    // bumps the counter. With `RateBounds::new(0)` (the harness default) it never
    // fires and this test would be vacuous.
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        empty_cache().await?.0,
        store_dyn,
        RATE_PER_MB,
        |deps| {
            deps.rate_bounds = decdn_node::rate_bounds::RateBounds::new(RATE_PER_MB * 50);
            deps.pull_through = Some(std::time::Duration::from_secs(5));
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let ext = binding_ext(&signer, client_node_id)?;
    let req = StreamRequest {
        hash: [0x18u8; 32],
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x1518,
    };
    let _ = raw_request(&client_ep, target, &req, Some(&ext)).await?;

    anyhow::ensure!(
        metric_line_present(&metrics.encode()?, "decdn_rate_bounds_clamp_events_total 1"),
        "one refused request must clamp exactly once; got:\n{}",
        metrics.encode()?
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The other half of #1518's invariant: a request that never quotes a rate must
/// not clamp at all. This is what pins the *placement* of `clamped_rate()` below
/// the binding block — hoisting it to the top of `serve_stream` would meter a clamp
/// for a request that is reset without a signed `StreamResponse`, which is the same
/// double-count bug in a different direction and is otherwise guarded only by prose.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_rejected_before_pricing_does_not_clamp_the_rate() -> anyhow::Result<()> {
    let (store, _signer, _deposit) = seeded_store()?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        empty_cache().await?.0,
        store,
        RATE_PER_MB,
        |deps| {
            deps.rate_bounds = decdn_node::rate_bounds::RateBounds::new(RATE_PER_MB * 50);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // A binding whose signature recovers a different address: the handler resets the
    // stream in the binding block, above the pricing point, signing nothing.
    let client_sk = fresh_key();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let mut ext = binding_ext(
        &Arc::new(PrivateKeySigner::random()),
        B256::repeat_byte(0x77),
    )?;
    if let Some(binding) = ext.binding.as_mut() {
        binding.ethereum_address = [0xAB; 20];
    }
    let req = StreamRequest {
        hash: [0x19u8; 32],
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x1519,
    };
    // The stream is reset, so there is no reply to read — that IS the expected shape.
    let _ = raw_request(&client_ep, target, &req, Some(&ext)).await;

    anyhow::ensure!(
        metric_line_present(&metrics.encode()?, "decdn_rate_bounds_clamp_events_total 0"),
        "a request reset above the pricing point must never clamp; got:\n{}",
        metrics.encode()?
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// ADR 011 keys compliance on the FUNDER, so a blacklisted funder must not make
/// this node front upstream USDC even when a perfectly clean delegate key is
/// doing the signing. The delegate's binding satisfies the *spend-authority*
/// half of `pull_authorized`; the funder check is the only thing standing
/// between a sanctioned address and this operator's egress bill.
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_refuses_a_blacklisted_funder_behind_a_clean_delegate() -> anyhow::Result<()> {
    let (store, funder, delegate) = delegate_signer_store()?;
    let (target, hits, server_ep, server_task, _cache_tmp, _metrics) =
        spawn_counting_pull_server_with_deny(
            store,
            &[funder.address()],
            U256::MAX,
            funder.address(),
        )
        .await?;

    let delegate_sk = fresh_key();
    let delegate_node_id = B256::from(*delegate_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(delegate_sk, vec![]).await?;
    let ext = binding_ext(&delegate, delegate_node_id)?;
    let req = StreamRequest {
        hash: [0xB1u8; 32], // never cached — a miss that would trigger the pull
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x00b1_ac11,
    };
    let _ = raw_request(&client_ep, target, &req, Some(&ext)).await?;
    anyhow::ensure!(
        hits.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "a blacklisted funder must NOT trigger a paid pull, clean delegate or not"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let (store, funder, delegate) = delegate_signer_store()?;
    // The funder (pool owner) is clean; only the delegate signer is on the
    // deny-set, and the signer's membership is not a compliance event.
    let (target, hits, server_ep, server_task, _cache_tmp, _metrics) =
        spawn_counting_pull_server_with_deny(
            store,
            &[delegate.address()],
            U256::MAX,
            funder.address(),
        )
        .await?;

    let delegate_sk = fresh_key();
    let delegate_node_id = B256::from(*delegate_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(delegate_sk, vec![]).await?;
    let ext = binding_ext(&delegate, delegate_node_id)?;
    let req = StreamRequest {
        hash: [0xB2u8; 32], // never cached — a miss that would trigger the pull
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
/// store (probed via a cloned cache handle after the response settles). Only the
/// foreground outer-deadline path can fill the store — there is no background warm
/// (#1610) that could fill it after the fact and mask the broken case.
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
    let server_eth = operator_signer();
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
        pool_id: pool_id().into(),
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
    shutdown([], [&client_ep]).await?;

    let filled = cache_probe.has(want).await?;
    shutdown([server_task], [&server_ep]).await?;
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
    store: Arc<dyn PoolStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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

/// Spawn a `ClientHandler` server with `relay_foreign_namespaces = false`
/// (#1759): an origin-only node. Reactive fs-origin pull-through
/// (`ClientHandlerDeps.pull_through`) is wired the same as
/// [`spawn_pull_through_server`] so a hash the backend genuinely holds can
/// still be served; the only difference is the origin-only gate.
async fn spawn_origin_only_server(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
            deps.pull_through = Some(Duration::from_secs(20));
            deps.relay_foreign_namespaces = false;
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_eth.address(), server_ep, server_task))
}

/// Same as [`spawn_origin_only_server`] but also returns the server's
/// [`Metrics`], so a test can tell a policy decline (`ForeignNamespaceDeclined`)
/// apart from a backend fault (`InternalError`) by the operator's own counters,
/// not just by the wire error (#1766).
async fn spawn_origin_only_server_with_metrics(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
) -> anyhow::Result<(
    EndpointAddr,
    Address,
    Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<Metrics>,
)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
            deps.pull_through = Some(Duration::from_secs(20));
            deps.relay_foreign_namespaces = false;
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

/// #1766: an origin-only node whose own backend FAULTS on the live probe (a
/// connection-refused `HEAD` against an unreachable http origin) must refuse
/// with `InternalError`, NOT a signed `NotFound` — a fault is not an
/// absence, and signing an authoritative negative during a backend outage
/// would tell a paying client this node's own content is gone.
///
/// `http://127.0.0.1:1/` is used as the "unreachable" origin: port 1 is a
/// privileged, essentially never-listening port, so `reqwest` gets an
/// immediate connection-refused rather than a slow timeout — the origin-chain
/// walk in `origin_probe_presence` sees a live `Err`, not the timeout arm
/// (that arm is already covered by the cache-engine unit tests).
#[tokio::test(flavor = "multi_thread")]
async fn origin_only_node_fault_is_internal_error_not_signed_not_found() -> anyhow::Result<()> {
    let hash = decdn_cache::Hash::new(b"origin-only fault probe");
    let cache_dir = tempfile::tempdir()?;
    let unreachable = decdn_cache::parse_origin_url("http://127.0.0.1:1/")?;
    let origin = Arc::new(decdn_cache::HttpOrigin::new(unreachable)?);
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

    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task, metrics) =
        spawn_origin_only_server_with_metrics(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, Arc::clone(&signer), deposit);

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
        Ok(_) => anyhow::bail!("a faulting backend probe must not deliver"),
        Err(e) => {
            let msg = e.to_string();
            anyhow::ensure!(
                msg.contains("InternalError"),
                "a faulting backend probe must surface as InternalError, got: {e}"
            );
            anyhow::ensure!(
                !msg.contains("NotFound"),
                "a faulting backend probe must NOT sign an authoritative NotFound, got: {e}"
            );
        }
    }

    let encoded = metrics.encode()?;
    anyhow::ensure!(
        metric_line_present(
            &encoded,
            "decdn_serve_stream_rejected_internal_error_total 1"
        ),
        "expected the fault to land in the internal-error counter, not the \
         foreign-decline one; counters were:\n{}",
        encoded
            .lines()
            .filter(|l| l.starts_with("decdn_serve_stream_rejected"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    anyhow::ensure!(
        metric_line_present(
            &encoded,
            "decdn_serve_stream_rejected_foreign_declined_total 0"
        ),
        "a backend fault must not be counted as a foreign decline"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Build a cache holding a blob that the local STORE has already accepted (a
/// past import) but whose backing filesystem origin no longer exists — the
/// backend genuinely does not hold it anymore, so a fresh
/// `CacheEngine::origin_probe_size` reads `None` (a transport error against
/// the now-deleted origin directory) even though `cache.has` is `true`. Used
/// to prove the origin-only gate (#1759) sits ABOVE the cache-hit branch: a
/// foreign hash already sitting in the store is still declined.
async fn cache_with_foreign_blob_imported(
    payload: &[u8],
) -> anyhow::Result<(
    CacheEngine,
    decdn_cache::Hash,
    Arc<CacheMetrics>,
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
    let _ = cache.get(hash).await?; // pulls into the local store now
    anyhow::ensure!(
        cache.has(hash).await?,
        "precondition: the blob must be in the local store"
    );
    // Drop the fs origin directory: the backend no longer holds the object,
    // even though the store already imported it.
    drop(origin_dir);
    Ok((cache, hash, cache_metrics, cache_dir))
}

/// #1759: an origin-only node declines a hash its own backend does not hold,
/// even for a request carrying a valid client binding — the gate is
/// backend-authoritative, not a binding/authorization check.
#[tokio::test(flavor = "multi_thread")]
async fn origin_only_node_declines_a_foreign_hash() -> anyhow::Result<()> {
    // Backend holds `payload`/`hash`; we request a DIFFERENT (foreign) hash.
    let payload = vec![0x11u8; 64 * 1024];
    let (cache, _own_hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let foreign_hash = decdn_cache::Hash::from([0x9Au8; 32]);

    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_origin_only_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, Arc::clone(&signer), deposit);

    let res = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *foreign_hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await;
    anyhow::ensure!(res.is_err(), "origin-only node must decline a foreign hash");

    // Declined BEFORE any origin egress / discovery.
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 0,
        "foreign decline must not touch the origin, got {}",
        cache_metrics.origin_fetches.get()
    );
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1759 control: an origin-only node still serves a hash its own backend
/// holds — the gate refuses only foreign content, not everything.
#[tokio::test(flavor = "multi_thread")]
async fn origin_only_node_serves_its_own_backend_hash() -> anyhow::Result<()> {
    let payload = vec![0x22u8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_origin_only_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, Arc::clone(&signer), deposit);

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
        "delivered bytes mismatch for own-backend hash"
    );
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 1,
        "expected exactly 1 origin fetch to fill the own-backend hash, got {}",
        cache_metrics.origin_fetches.get()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// #1759: the gate is categorical — it sits ABOVE the cache-hit branch, so a
/// foreign hash already sitting in the store (imported by an earlier relay,
/// say) is still declined once the backend no longer holds it.
#[tokio::test(flavor = "multi_thread")]
async fn origin_only_node_declines_a_foreign_blob_already_in_cache() -> anyhow::Result<()> {
    let payload = vec![0x33u8; 32 * 1024];
    let (cache, foreign_hash, cache_metrics, _cache_tmp) =
        cache_with_foreign_blob_imported(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_origin_only_server(cache, Arc::clone(&store)).await?;

    // Baseline AFTER setup's own origin fetch (from importing the blob into the
    // store), so the assertion below isolates fetches caused by THIS request.
    let baseline_fetches = cache_metrics.origin_fetches.get();

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(&client_ep, Arc::clone(&signer), deposit);

    let res = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth_addr,
        *foreign_hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await;
    anyhow::ensure!(
        res.is_err(),
        "origin-only node must decline a foreign hash even when it is already cached"
    );
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == baseline_fetches,
        "the cache-hit gate must not touch the origin, got {} beyond baseline {}",
        cache_metrics.origin_fetches.get(),
        baseline_fetches
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// ADR 041: the buy-side serve-economics gate (the `margin` policy's buy
/// ceiling + the per-source [`decdn_node::warming_allowance::WarmingAllowance`])
/// lives ONLY on `NodeOrigin`'s peer-candidate relay path — the buy loop that
/// decides what to pay ANOTHER node. It is never consulted on the own-namespace
/// path: filling from this node's OWN configured origin is not a purchase, so
/// there is nothing to gate.
///
/// The source's warming allowance is drained to fully spent and the operator
/// fee share pinned at 0 bps before the fetch — settings that, under the
/// `margin` policy, refuse EVERY peer-relay candidate at any positive quote —
/// yet the own-origin fetch still serves the blob intact, proving the own-
/// namespace path never reads either.
#[tokio::test(flavor = "multi_thread")]
async fn own_namespace_miss_ignores_serve_economics_gate() -> anyhow::Result<()> {
    let payload = vec![0x9Eu8; 64 * 1024];
    let (cache, hash, cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);

    // The most restrictive possible ADR 041 settings: an arbitrary source's
    // warming allowance fully drained, and a 0 bps operator fee share (so even
    // the amortized "safe" floor collapses to 0). If the own-origin path
    // consulted either, this fetch would refuse.
    let warming = Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
        1000, 0,
    ));
    warming.debit_speculative([0xAAu8; 32], [0xBBu8; 32], u64::MAX);
    anyhow::ensure!(
        !warming.available([0xAAu8; 32]),
        "precondition: the source must read as fully spent"
    );

    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store.clone(),
        RATE_PER_MB,
        0,
        16,
        |deps| {
            deps.pull_through = Some(Duration::from_secs(20));
            deps.warming_credit = Arc::new(
                decdn_node::warming_allowance::DirectWarmingCreditSink::new(Arc::clone(&warming)),
            );
            deps.operator_shares = decdn_node::fee_shares::OperatorShares::new(0);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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
    anyhow::ensure!(
        cache_metrics.origin_fetches.get() == 1,
        "expected exactly 1 origin fetch (own-namespace path taken), got {}",
        cache_metrics.origin_fetches.get()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// ADR 041: a completed serve credits the warming source through the SHIPPED
/// path — the bounded channel and background aggregator the runtime wires — not
/// just through the inline test sink.
///
/// Every other ADR 041 test here reads the ledger synchronously and so uses
/// `DirectWarmingCreditSink`. That leaves the production wiring
/// (`credit_warming_serve` -> resolve the source -> `try_send` -> aggregator ->
/// `credit_source`) proven by unit tests alone: a regression that dropped the
/// runtime's `spawn_warming_creditor` call, or an aggregator that never drained,
/// would keep every one of them green. This drives the real sink end-to-end over
/// a real paid fetch, and polls because the apply is asynchronous by design.
#[tokio::test(flavor = "multi_thread")]
async fn a_completed_serve_credits_the_source_through_the_background_aggregator()
-> anyhow::Result<()> {
    const SOURCE: [u8; 32] = [0xA1u8; 32];
    const OP_BPS: u16 = 6000;

    let payload = vec![0x5Cu8; 64 * 1024];
    let (cache, hash, _cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    let (store, signer, deposit) = seeded_store()?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);

    // Tag the blob to SOURCE with a one-unit speculative debit: the source reads
    // as spent until a serve credits it, and any positive margin vindicates it.
    // So `available` flips exactly when the aggregator applies the credit, which
    // is the signal this test waits on.
    let warming = Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
        1_000_000, 0,
    ));
    warming.debit_speculative(SOURCE, *hash.as_bytes(), 1);
    anyhow::ensure!(
        !warming.available(SOURCE),
        "precondition: the speculative buy must leave the source spent"
    );

    let (warming_credit, creditor) = decdn_node::warming_allowance::spawn_warming_creditor(
        Arc::clone(&warming),
        Arc::clone(&metrics),
    );

    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store.clone(),
        RATE_PER_MB,
        0,
        16,
        |deps| {
            deps.pull_through = Some(Duration::from_secs(20));
            deps.warming_credit = warming_credit;
            deps.operator_shares = decdn_node::fee_shares::OperatorShares::new(OP_BPS);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let own_node_id = B256::from(*client_ep.id().as_bytes());
    let binding = sign_client_binding(&signer, own_node_id, &binding_domain())?;
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

    let got = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *hash.as_bytes(),
        0,
        0x00c0_dec0,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "delivered bytes mismatch"
    );

    let mut credited = false;
    for _ in 0..500 {
        if warming.available(SOURCE) {
            credited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::ensure!(
        credited,
        "the serve credit never reached the ledger through the channel sink"
    );

    anyhow::ensure!(
        creditor.shutdown().await.is_ok(),
        "the aggregator must exit cleanly"
    );
    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx = unbound_context(Arc::clone(&signer), deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A serve miss on a hash the node **advertises as origin-held** answers with a
/// signed `StreamResponse{ok: false}` rather than dropping the stream.
///
/// This is the path the #1130 fail-silent gate used to suppress. That gate
/// existed because a signed `has_blob: true` probe plus a signed `ok: false`
/// stream was phantom-announcement evidence; with the phantom offense retired
/// the refusal is inert (rate manipulation requires `ok == true`, blacklist
/// violation requires a served claim — ADR 014), so the accountable answer is to
/// sign it. Dropping instead cost the requester a full open-stage timeout.
///
/// The setup is the [`unbound_client_fetch_is_refused_on_origin_only_blob`]
/// control plus the one thing it lacks: `rescan_origins()`, which populates the
/// `origin_held` index so `origin_held_size(hash)` is `Some`. Without that call
/// the suppressed branch was unreachable, which is why no test ever covered it.
#[tokio::test(flavor = "multi_thread")]
async fn origin_held_serve_miss_signs_a_refusal_rather_than_dropping() -> anyhow::Result<()> {
    let payload = vec![0x3Cu8; 32 * 1024];
    let (cache, hash, _cache_metrics, _origin_tmp, _cache_tmp) =
        empty_cache_with_fs_origin(&payload).await?;
    // Populate the origin-held index: the node now advertises this hash on
    // probe (`has_blob: true`) even though the store has never held it.
    cache.rescan_origins().await;
    anyhow::ensure!(
        cache.origin_held_size(hash) == Some(payload.len() as u64),
        "precondition: the hash must be indexed as origin-held"
    );
    let (store, signer, deposit) = seeded_store()?;
    let (target, server_eth_addr, server_ep, server_task) =
        spawn_pull_through_server(cache, Arc::clone(&store)).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    // No client binding => `pull_authorized` fails, so the reactive pull never
    // runs and the serve path reaches a plain `CacheMiss` on origin-held content.
    let ctx = unbound_context(Arc::clone(&signer), deposit);

    let Err(err) = stream_fetch(
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
    else {
        anyhow::bail!("an unbound fetch of origin-held content must be refused")
    };

    // The assertion that distinguishes this from the deleted behaviour: a typed
    // refusal carrying the operator's signed response, not a timeout on a
    // silently-dropped stream.
    let refused = err
        .downcast_ref::<decdn_client_pull::UpstreamRefused>()
        .ok_or_else(|| anyhow::anyhow!("expected a typed UpstreamRefused, got: {err:#}"))?;
    let evidence = refused.evidence().ok_or_else(|| {
        anyhow::anyhow!("the refusal must carry the operator's signed StreamResponse")
    })?;
    anyhow::ensure!(
        !evidence.body.ok,
        "an origin-held serve miss must sign ok:false, got ok:{}",
        evidence.body.ok
    );
    anyhow::ensure!(
        evidence.body.hash == *hash.as_bytes(),
        "the signed refusal must name the requested hash"
    );
    anyhow::ensure!(
        !evidence.slash_sig.is_empty(),
        "the refusal must be signed — an unsigned one is unattributable"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Like [`spawn_pull_through_server`] but arms ONLY the reactive LOCAL-origin
/// populate (`ClientHandlerDeps.local_populate`) — NOT the node→node buffered/window paths.
/// This is the cache-only-operator wiring (#1116): `[cache.origin]` set,
/// `node_to_node_pull_through_enabled` off.
async fn spawn_local_populate_server(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
    store: Arc<dyn PoolStateStore>,
) -> anyhow::Result<(EndpointAddr, Address, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
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
            deps.pull_through_origin = Some(Arc::new(decdn_node::node_origin::NodeOrigin::new()));
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    // No client binding: reactive local populate must stay gated on proven
    // channel ownership, so the unbound request is refused.
    let ctx = unbound_context(Arc::clone(&signer), deposit);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    store: Arc<dyn PoolStateStore>,
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
    let server_eth = operator_signer();
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
                deps.pull_through_origin =
                    Some(Arc::new(decdn_node::node_origin::NodeOrigin::new()));
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
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
    let ctx =
        channel_context(&client_ep, Arc::clone(&signer), deposit).with_client_binding(binding);

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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// `spawn_pool_server_with_loss` but wired over an EXTERNALLY supplied loss
/// store instead of a fresh in-memory one — the durable-restart test needs the
/// SAME store handle to back both the lane table and the floor-loss table, so
/// `record_loss`/`load_losses` persist to the caller's own redb file rather
/// than an ephemeral in-memory one.
async fn spawn_pool_server_with_stores(
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    loss: Arc<dyn decdn_incentive::PoolFloorLossStore>,
    remaining: U256,
) -> anyhow::Result<(EndpointAddr, Endpoint, tokio::task::JoinHandle<()>)> {
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let owner = operator_addr();
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store,
        RATE_PER_MB,
        |deps| {
            deps.pool_view = Some(Arc::new(FixedRemainingPoolView { owner, remaining }));
            deps.credit_max = decdn_common::config::DEFAULT_CREDIT_MAX;
            deps.credit_ramp_divisor = decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR;
            deps.floor_loss_store = Some(loss);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    Ok((target, server_ep, server_task))
}

/// Poll a [`PersistentPoolStateStore`] directly (not the in-memory shortcut
/// `await_pool_dead_charge` uses) until [`pool_id`]'s durable `dead_charge`
/// reaches `want`, or bail on overshoot / a 10 s stall. Mirrors
/// `await_pool_dead_charge`'s deterministic wait-for-value discipline so the
/// restart test never guesses at a fixed sleep for the `Drop` guard's
/// off-task `spawn_blocking` fold to land.
async fn await_persistent_pool_dead_charge(
    store: &PersistentPoolStateStore,
    want: u128,
) -> anyhow::Result<()> {
    use decdn_incentive::PoolFloorLossStore as _;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let current = pool_dead_charge_total(store.load_losses()?);
        if current == want {
            return Ok(());
        }
        anyhow::ensure!(
            current <= want,
            "pool dead_charge {current} overshot the expected {want}"
        );
        anyhow::ensure!(
            Instant::now() < deadline,
            "pool dead_charge stalled at {current}, expected {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `dead_charge` survives a handler rebuild (simulated restart): withhold a
/// floor on lane A, let the reservation's `Drop` reconcile and durably persist
/// the fold to redb, drop the handler AND the redb handle, reopen the SAME
/// `data_dir` as a fresh [`PersistentPoolStateStore`], and rebuild the handler
/// over it. Handler construction hydrates `dead_charge` from
/// `floor_loss_store.load_losses()`, so a brand-new distinct lane B — never
/// touched before the restart — is still refused: the persisted `dead_charge`
/// alone consumes the pool's whole free-floor budget. A design that forgot to
/// persist (or failed to rehydrate) `dead_charge` would admit B on a fresh
/// in-memory zero, and this refusal would not happen — that is exactly the
/// "withhold then restart" escape this test rules out.
///
/// `remaining` covers one `HARNESS_FLOOR_COST` and no more, `M = 0`: lane A's withheld
/// floor folds `dead_charge` to `40`. A second floor would need
/// `40 + 40 = 80 > 60`, so lane B is refused after the restart, even though it
/// never touched the pool before.
#[tokio::test(flavor = "multi_thread")]
async fn dead_charge_persists_across_restart() -> anyhow::Result<()> {
    use decdn_incentive::PoolFloorLossStore as _;

    let payload = vec![0x7Cu8; 6 * 1024 * 1024];

    let dir = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    }

    // Slack strictly under one floor: enough for lane A's floor and no more.
    let remaining = U256::from(u128::from(HARNESS_FLOOR_COST) + 2);
    let signer_a = Arc::new(PrivateKeySigner::random());
    let signer_b = Arc::new(PrivateKeySigner::random());

    // --- "Boot 1": withhold lane A's voucher, fold its floor into dead_charge. ---
    {
        let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
        let redb_store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        redb_store.record(&fresh_lane(signer_a.address(), U256::from(10_000_000u64)))?;
        let store_dyn: Arc<dyn PoolStateStore> = redb_store.clone();
        let loss_dyn: Arc<dyn decdn_incentive::PoolFloorLossStore> = redb_store.clone();

        let (target, server_ep, server_task) =
            spawn_pool_server_with_stores(cache, store_dyn, loss_dyn, remaining).await?;

        let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
        let client_node_id = B256::from(*client_ep.id().as_bytes());
        let conn = client_ep
            .connect(target, ALPN_CLIENT)
            .await
            .map_err(|e| anyhow::anyhow!("connect lane A: {e}"))?;
        let ext = binding_ext(&signer_a, client_node_id)?;
        let (send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;
        read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
        assert_parked_awaiting_voucher(&mut recv).await?;
        // Disconnect while withholding: the server's read errors, its serve
        // returns, and the `FloorReservation` folds one floor into redb.
        drop(send);
        drop(recv);
        conn.close(0u32.into(), b"withhold");

        await_persistent_pool_dead_charge(&redb_store, u128::from(HARNESS_FLOOR_COST)).await?;

        shutdown([server_task], [&client_ep, &server_ep]).await?;
        // `redb_store` (and every other reference to it) drops at the end of
        // this block, releasing redb's process-exclusive lock on `dir.path()`
        // before the reopen below — reopening while still held returns
        // `StoreError::Backend("... AlreadyOpen ...")`.
    }

    // --- Simulated restart: reopen the SAME redb file, rebuild the handler. ---
    let (cache_b, hash_b, _cache_tmp_b) = cache_with_blob(&payload).await?;
    let reopened = Arc::new(PersistentPoolStateStore::open(dir.path())?);

    // The reopened store shows the persisted dead_charge on its own, independent
    // of whatever handler #2 hydrates internally.
    let persisted = pool_dead_charge_total(reopened.load_losses()?);
    anyhow::ensure!(
        persisted == u128::from(HARNESS_FLOOR_COST),
        "expected the durable dead_charge to survive reopen, got {persisted:?}"
    );
    // And it survives keyed by SIGNER, not merely as a pool total: the per-signer
    // sub-cap (ADR 003 §Pool solvency) is only carried across a restart if the
    // durable row remembers which capability-holder owes the charge. A row keyed
    // by pool alone would rehydrate every signer's share as one undifferentiated
    // pool total and lose the isolation.
    let rows = reopened.load_losses()?;
    anyhow::ensure!(
        rows == vec![(
            pool_id(),
            signer_a.address(),
            u128::from(HARNESS_FLOOR_COST)
        )],
        "the dead charge must reload against the signer that incurred it, got {rows:?}"
    );

    reopened.record(&fresh_lane(signer_b.address(), U256::from(10_000_000u64)))?;
    let store_dyn: Arc<dyn PoolStateStore> = reopened.clone();
    let loss_dyn: Arc<dyn decdn_incentive::PoolFloorLossStore> = reopened.clone();

    let (target_b, server_ep_b, server_task_b) =
        spawn_pool_server_with_stores(cache_b, store_dyn, loss_dyn, remaining).await?;

    let (client_ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let client_node_id_b = B256::from(*client_ep_b.id().as_bytes());
    let conn_b = client_ep_b
        .connect(target_b, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect lane B: {e}"))?;
    let ext_b = binding_ext(&signer_b, client_node_id_b)?;

    // Lane B is BRAND NEW — never touched before the restart — yet handler #2's
    // hydrated `dead_charge` alone consumes the pool's free-floor budget, so it
    // is refused. A fresh in-memory `dead_charge` (the bug this test guards
    // against) would admit it instead.
    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn_b, *hash_b.as_bytes(), Some(&ext_b)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "lane B must be refused: the restored dead_charge already spends the free-floor budget"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "expected the collapsed NotFound wire code, got {:?}",
        refusal_ext.error
    );

    conn_b.close(0u32.into(), b"done");
    client_ep_b.close().await;
    server_ep_b.close().await;
    server_task_b.await?;
    Ok(())
}

/// End-to-end coverage for the load-shed gate's two `Err` branches in
/// `serve_stream` (dispatch.rs): the cache-miss admission call refuses with
/// `ServeRejectReason::LoadShedMiss`, and the cache-hit admission call refuses
/// with `ServeRejectReason::LoadShedHit`. Both collapse to the same signed wire
/// `NotFound` a paying client sees. The policy's own admit/shed arithmetic
/// already has unit coverage in `load_shed::resource_pressure`; what these two
/// tests add is proof the handler actually wires a shed `Err` into the exact
/// on-wire refusal, through a real loopback connection and a real bound
/// request.
///
/// Both tests pre-set the controller's counters before the client ever
/// connects, so the shed decision is already fixed at admission time — no
/// timing or concurrency race is involved.
///
/// A hash the node genuinely lacks: the request lands on the cache-miss
/// admission call. The node's concurrency ceiling is pre-occupied by a held
/// slot, so it is already at its one-slot high-water mark.
#[tokio::test(flavor = "multi_thread")]
async fn cache_miss_refused_under_concurrency_pressure() -> anyhow::Result<()> {
    let (cache, _cache_tmp) = empty_cache().await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&fresh_lane(signer.address(), U256::from(10_000_000u64)))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    // Concurrency high-water mark of 1: pre-occupying one slot before the
    // client connects puts the node at capacity for every request that follows.
    let shed = decdn_node::load_shed::LoadShedController::from_config(
        &decdn_common::config::ResolvedLoadShed {
            policy: decdn_common::config::LoadShedPolicyKind::ResourcePressure,
            egress_budget_mbps: 0,
            max_concurrent_serves_high: 1,
            max_concurrent_serves_low: 0,
            per_client_serve_cap: 0,
        },
    );
    let held = shed
        .try_admit(
            decdn_node::load_shed::RequestClass::CacheHit,
            B256::repeat_byte(0xAA),
        )
        .map_err(|reason| anyhow::anyhow!("pre-occupy the sole concurrency slot: {reason:?}"))?;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        |deps| {
            deps.shed = Arc::clone(&shed);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = binding_ext(&signer, client_node_id)?;

    // A hash the empty cache never has: the cache-miss admission branch.
    let miss_hash = Hash::new(b"load-shed test: never cached");
    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn, *miss_hash.as_bytes(), Some(&ext)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "a cache-miss request must be shed while the node is at its concurrency ceiling"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "expected the collapsed NotFound wire code, got {:?}",
        refusal_ext.error
    );

    // Release the held slot only after the refusal is observed, so the shed
    // decision above is provably driven by the pre-occupied slot.
    drop(held);
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A hash the node already has cached: the request lands on the cache-hit
/// admission call. Concurrency never trips (the high/low water marks are far
/// above anything this test does); only the egress ceiling is saturated, which
/// `ResourcePressure` sheds regardless of the pressure latch (it also sheds
/// hits, unlike the concurrency gate above).
#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_refused_under_egress_saturation() -> anyhow::Result<()> {
    let payload = b"load-shed test: a blob the node already has cached".to_vec();
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&fresh_lane(signer.address(), U256::from(10_000_000u64)))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    let shed = decdn_node::load_shed::LoadShedController::from_config(
        &decdn_common::config::ResolvedLoadShed {
            policy: decdn_common::config::LoadShedPolicyKind::ResourcePressure,
            egress_budget_mbps: 1, // 1 Mbps = 125_000 B/s ceiling
            max_concurrent_serves_high: 10_000,
            max_concurrent_serves_low: 9_000,
            per_client_serve_cap: 0,
        },
    );
    // Pre-feed the egress meter over budget before the client ever connects,
    // so the instant rate the handler samples at admission time is already
    // fixed above the ceiling.
    shed.record_egress(1_000_000);
    let sampled = shed.sample_egress(1);
    anyhow::ensure!(
        sampled >= 125_000,
        "instant egress rate must be at/above the 1 Mbps ceiling, got {sampled}"
    );

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let handler = build_handler_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        |deps| {
            deps.shed = Arc::clone(&shed);
        },
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = binding_ext(&signer, client_node_id)?;

    let (refusal, refusal_ext) =
        open_expecting_refusal(&conn, *hash.as_bytes(), Some(&ext)).await?;
    anyhow::ensure!(
        !refusal.body.ok,
        "a cache-hit request must be shed while measured egress is at the configured ceiling"
    );
    anyhow::ensure!(
        matches!(
            refusal_ext.error,
            Some(decdn_protocol::client::StreamError::NotFound)
        ),
        "expected the collapsed NotFound wire code, got {:?}",
        refusal_ext.error
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Hostile-client and hostile-server paths (#1562)
//
// The payment-fault tests reuse [`StalledDelivery`] / the raw pipelined stream
// helpers to reimplement the payer half by hand, so a test chooses exactly which
// malformed proof it sends and when. The client-side detection tests instead run
// the real [`stream_fetch`] requester against [`serve_lying_stream`], a raw
// server that signs a valid `StreamResponse` and then misbehaves on the byte
// stream. Both extend the honest scaffolding above rather than duplicating it.
// ---------------------------------------------------------------------------

/// Sign a cumulative voucher carrying an explicit `(bytes_delivered, amount)` and
/// write it to `send`. Unlike [`pay_cumulative`] (which prices `amount` at the
/// quoted rate) this hands the caller both fields, so a test can inject a voucher
/// that regresses or diverges from the lane watermark. Signed correctly by the
/// lane's pinned key, so the node's signature recovery passes and the reject is
/// decided by the watermark checks.
async fn send_signed_voucher(
    send: &mut SendStream,
    signer: &PrivateKeySigner,
    bytes_delivered: u64,
    amount: U256,
) -> anyhow::Result<()> {
    let voucher = Voucher {
        pool_id: pool_id(),
        signer: signer.address(),
        provider: operator_addr(),
        amount,
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(signer, &payment_domain())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client_msg(
        send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&voucher)?),
    )
    .await
}

/// Write a voucher whose signature bytes do not recover to any point (`r == s ==
/// 0`, the canonical unrecoverable case) so the node rejects it `BadSignature`
/// (`VoucherError::InvalidSignature`).
///
/// It is derived from a real, correctly-priced voucher and only its signature is
/// zeroed, so the length still clears the wire `validate()` gate — the reject is
/// the recovery failing, not a malformed frame. A hostile payer is not bound by
/// our constructors, so the forgery is modelled on the wire.
async fn send_unrecoverable_voucher(
    send: &mut SendStream,
    signer: &PrivateKeySigner,
    bytes_delivered: u64,
) -> anyhow::Result<()> {
    let amount = min_payment(bytes_delivered, RATE_PER_MB);
    let voucher = Voucher {
        pool_id: pool_id(),
        signer: signer.address(),
        provider: operator_addr(),
        amount,
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(signer, &payment_domain())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    let mut wire = signed_to_wire_voucher(&voucher)?;
    // Keep the length (so `Voucher::validate` passes) but blank the bytes: `r ==
    // s == 0` parses structurally yet recovers to nothing → `InvalidSignature`.
    wire.signature = vec![0u8; wire.signature.len()];
    write_client_msg(send, &ClientMessage::Voucher(wire)).await
}

/// Read past any in-flight `ChunkData` to the node's mid-stream `VoucherRejected`,
/// returning its reason and whether a wallet-less-resume [`WatermarkBundle`] rode
/// along. The bundle-carrying twin of [`expect_reject`], which asserts the bundle
/// is absent; a watermark-regression reject on a lane with an accepted voucher
/// carries one (#1481 §5), so this hands the flag back for the caller to assert.
async fn read_voucher_reject(recv: &mut RecvStream) -> anyhow::Result<(VoucherRejectReason, bool)> {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), read_client_msg(recv))
            .await
            .map_err(|_| anyhow::anyhow!("node never answered with a rejection"))??;
        match msg {
            ClientMessage::ChunkData(_) => {}
            ClientMessage::StreamError(StreamError::VoucherRejected { reason, bundle }) => {
                return Ok((reason, bundle.is_some()));
            }
            other => anyhow::bail!("expected a VoucherRejected, got {other:?}"),
        }
    }
}

/// Assert `recv` observes a QUIC reset (or connection close) carrying `code`,
/// with no readable frame first. Mirrors the probe suite's reset assertion: the
/// stream-cap shed resets with [`decdn_protocol::APP_ERR_RATE_LIMITED`] and signs
/// no `StreamResponse`, so a returned frame is itself the failure.
async fn assert_stream_reset(recv: &mut RecvStream, code: u32) -> anyhow::Result<()> {
    let expected = VarInt::from_u32(code);
    match recv.read_to_end(4096).await {
        Err(ReadToEndError::Read(ReadError::Reset(got))) if got == expected => Ok(()),
        Err(ReadToEndError::Read(ReadError::ConnectionLost(
            ConnectionError::ApplicationClosed(ApplicationClose { error_code, .. }),
        ))) if error_code == expected => Ok(()),
        Ok(bytes) => anyhow::bail!(
            "expected a reset carrying {code:#x}, but the server sent {} bytes (it signed a \
             response instead of shedding)",
            bytes.len()
        ),
        other => anyhow::bail!("expected a reset carrying {code:#x}, got {other:?}"),
    }
}

/// An underpaying closing voucher (correct cumulative bytes, an amount far below
/// the quoted-rate minimum) is a buyer withholding, not a protocol reject: the
/// node bails the serve loop with no in-band `StreamError` (ADR 003 §Voucher
/// withholding), so the stream ends in a reset with no `StreamEnd`, and — the
/// money half — the lane watermark never advances, so the underpaid bytes are
/// never credited.
#[tokio::test(flavor = "multi_thread")]
async fn underpaying_closing_voucher_fails_the_stream_and_credits_nothing() -> anyhow::Result<()> {
    // Under one credit-window floor (1 chunk) so the delivery has exactly one
    // (closing) voucher, but large enough that the quoted-rate minimum is well
    // above 1 — so `amount = 1` is an unambiguous underpayment, not a rounding tie.
    let payload = vec![0x7Eu8; 768 * 1024];
    let fx = idle_fixture(&payload, Duration::from_secs(30)).await?;
    let blob = fx.blob(0)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;
    let mut stalled = stall_delivery_at_closing_voucher(
        &conn,
        *blob.hash.as_bytes(),
        blob.wire_bytes,
        Some(&ext),
    )
    .await?;

    // The full byte count, but one base unit of payment: `min_payment` for the
    // whole delivery is far above 1, so this underpays the delivered span.
    let owed = min_payment(blob.wire_bytes, RATE_PER_MB);
    anyhow::ensure!(
        owed > U256::from(1u64),
        "fixture must owe more than one base unit for the underpay to be unambiguous"
    );
    let outcome = stalled
        .send_cumulative_voucher(&fx.client_signer, blob.wire_bytes, U256::from(1u64))
        .await;
    anyhow::ensure!(
        outcome.is_err(),
        "an underpaying voucher must bail the stream (a reset, no in-band reject frame), got {outcome:?}"
    );

    // The withholding is not merely refused — nothing is credited. The lane
    // watermark stays at zero, so the underpaid bytes cannot be settled later.
    let persisted = fx.store.load_all()?;
    if let Some(lane) = persisted.first() {
        anyhow::ensure!(
            lane.last_bytes_delivered() == U256::ZERO,
            "an underpaid stream must not advance the lane watermark, got {}",
            lane.last_bytes_delivered()
        );
        anyhow::ensure!(
            lane.last_amount() == U256::ZERO,
            "an underpaid stream must credit no amount, got {}",
            lane.last_amount()
        );
    }

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// Drive a raw pipelined stream to the point where one interval has been paid and
/// accepted and the next is on the wire awaiting its voucher — the shared setup
/// for the two "hostile proof after an accepted voucher" tests. Returns the live
/// stream halves plus the client endpoint / server task to tear down.
struct MidStreamAfterAccept {
    send: SendStream,
    recv: RecvStream,
    signer: Arc<PrivateKeySigner>,
    store: Arc<dyn PoolStateStore>,
    conn: Connection,
    client_ep: Endpoint,
    server_ep: Endpoint,
    server_task: tokio::task::JoinHandle<()>,
}

async fn drive_to_second_interval_awaiting_voucher() -> anyhow::Result<MidStreamAfterAccept> {
    const CREDIT_MAX: u64 = 64 * 1024 * 1024;
    // 16 MiB: many intervals, so a reject after interval 2 lands with plenty of
    // blob still undelivered — genuinely mid-stream, not the closing voucher.
    let payload = vec![0x9Cu8; 16 * 1024 * 1024];
    let (cache, hash, cache_tmp) = cache_with_blob(&payload).await?;
    std::mem::forget(cache_tmp); // keep the cache dir for the connection's life
    let (store, signer, _deposit) = seeded_store()?;

    // Divisor 2 keeps the window pinned at the one-interval floor after the first
    // interval is paid (`paid / 2 < floor`), so the server parks after each
    // interval — a deterministic voucher rendezvous.
    let (target, _server_eth, server_ep, server_task) =
        spawn_pipelined_server(cache, Arc::clone(&store), CREDIT_MAX, 2).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;
    let (mut send, mut recv) = open_paid_stream(&conn, *hash.as_bytes(), Some(&ext)).await?;

    // Interval 1: delivered on the floor window, then paid + accepted. The server
    // only widens past the floor after crediting this voucher, so reading
    // interval 2 below is itself the proof that interval 1 was accepted.
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;
    pay_cumulative(&mut send, &signer, HARNESS_INTERVAL_BYTES).await?;

    // Interval 2 arrives; the server then parks awaiting its voucher, which is
    // where the hostile proof is injected.
    read_exact_chunks(&mut recv, HARNESS_INTERVAL_BYTES).await?;

    Ok(MidStreamAfterAccept {
        send,
        recv,
        signer,
        store,
        conn,
        client_ep,
        server_ep,
        server_task,
    })
}

/// A `BadSignature` proof arriving *after* an accepted voucher is rejected in
/// band and the accepted watermark survives: signature recovery is the first
/// watermark-independent gate, so the reject is decided before any lane state is
/// touched, and it carries no wallet-less-resume bundle (the fix is a valid
/// signature, not a watermark resync).
#[tokio::test(flavor = "multi_thread")]
async fn bad_signature_after_an_accepted_voucher_is_rejected() -> anyhow::Result<()> {
    let mut fx = drive_to_second_interval_awaiting_voucher().await?;

    // A voucher for interval 2's cumulative, correctly priced but with an
    // unrecoverable signature.
    send_unrecoverable_voucher(&mut fx.send, &fx.signer, 2 * HARNESS_INTERVAL_BYTES).await?;
    let (reason, has_bundle) = read_voucher_reject(&mut fx.recv).await?;
    anyhow::ensure!(
        reason == VoucherRejectReason::BadSignature,
        "expected BadSignature, got {reason:?}"
    );
    anyhow::ensure!(
        !has_bundle,
        "a BadSignature reject carries no watermark bundle: the fix is a valid signature, not a resync"
    );

    // The accepted interval-1 voucher survives the reject: the watermark holds at
    // one interval, neither regressed nor advanced by the forged proof.
    let persisted = fx.store.load_all()?;
    let lane = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted lane after an accepted voucher"))?;
    anyhow::ensure!(
        lane.last_bytes_delivered() == U256::from(HARNESS_INTERVAL_BYTES),
        "the accepted watermark must survive the reject, got {}",
        lane.last_bytes_delivered()
    );

    fx.conn.close(0u32.into(), b"done");
    shutdown([fx.server_task], [&fx.client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// A watermark-regressing proof after an accepted voucher — same signed amount as
/// the accepted watermark but MORE bytes claimed, the single-signer divergence of
/// #1699 rule 4 — is rejected `BytesRegression`. It carries no wallet-less-resume
/// bundle: a divergent voucher from the pinned signer is that signer
/// misbehaving, not a stale local watermark a resync could repair, so the node
/// hands back nothing to re-sign from. The accepted watermark is unmoved.
#[tokio::test(flavor = "multi_thread")]
async fn regressing_voucher_after_an_accepted_voucher_is_rejected() -> anyhow::Result<()> {
    let mut fx = drive_to_second_interval_awaiting_voucher().await?;

    // Same money as the accepted interval-1 voucher, but claiming twice the bytes:
    // same amount + more bytes is a divergent fault, not a benign supersede.
    let accepted_amount = min_payment(HARNESS_INTERVAL_BYTES, RATE_PER_MB);
    send_signed_voucher(
        &mut fx.send,
        &fx.signer,
        2 * HARNESS_INTERVAL_BYTES,
        accepted_amount,
    )
    .await?;
    let (reason, has_bundle) = read_voucher_reject(&mut fx.recv).await?;
    anyhow::ensure!(
        reason == VoucherRejectReason::BytesRegression,
        "expected BytesRegression, got {reason:?}"
    );
    anyhow::ensure!(
        !has_bundle,
        "a divergent equal-amount voucher is a single-signer fault, not a resyncable watermark: no bundle"
    );

    let persisted = fx.store.load_all()?;
    let lane = persisted
        .first()
        .ok_or_else(|| anyhow::anyhow!("no persisted lane after an accepted voucher"))?;
    anyhow::ensure!(
        lane.last_bytes_delivered() == U256::from(HARNESS_INTERVAL_BYTES),
        "the accepted watermark must not move on a regression reject, got {}",
        lane.last_bytes_delivered()
    );

    fx.conn.close(0u32.into(), b"done");
    shutdown([fx.server_task], [&fx.client_ep, &fx.server_ep]).await?;
    Ok(())
}

/// The per-connection stream cap sheds an over-cap stream with a bare QUIC reset
/// carrying [`decdn_protocol::APP_ERR_RATE_LIMITED`] and signs no
/// `StreamResponse` (ADR 005 §Concurrent stream limits): signing a response per
/// shed request would let a request flood amplify into one ECDSA signature each,
/// so the cap adds no work to the reject path. One stream holds the sole permit
/// (parked at its closing voucher); a second stream on the same connection is
/// reset without a response.
#[tokio::test(flavor = "multi_thread")]
async fn over_cap_stream_is_reset_without_signing_a_response() -> anyhow::Result<()> {
    // 64 KiB: fits one credit-window floor, so the first stream parks at a single
    // closing voucher while holding the sole stream permit.
    let payload = vec![0x4Bu8; 64 * 1024];
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let wire_bytes = support::bao_wire_len_whole(payload.len() as u64);

    let signer = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&fresh_lane(signer.address(), U256::from(10_000_000u64)))?;
    let store_dyn: Arc<dyn PoolStateStore> = store.clone();

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = operator_signer();
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    // Per-connection cap of exactly one concurrent stream.
    let handler = build_handler_limited_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        0,
        1,
        |_deps| {},
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&signer, client_node_id)?;

    // Stream 1 takes the sole permit and parks at its closing voucher, holding it
    // for as long as the test declines to pay.
    let _held =
        stall_delivery_at_closing_voucher(&conn, *hash.as_bytes(), wire_bytes, Some(&ext)).await?;

    // Stream 2 on the SAME connection finds no permit: the handler resets it with
    // RATE_LIMITED and never signs a response.
    let (mut send2, mut recv2) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi (second stream): {e}"))?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id().into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x0012_61a0,
    };
    write_frame(&mut send2, &encode_stream_request(&req, Some(&ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write second request: {e}"))?;
    assert_stream_reset(&mut recv2, decdn_protocol::APP_ERR_RATE_LIMITED).await?;

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The first `chunks_frame` bytes of `wire`, framed as `ChunkData` messages of at
/// most [`LYING_FRAME`] bytes each — a raw upstream's byte stream, hand-rolled so
/// a test can then append more (over-send) or swap in the wrong bytes
/// (hash-mismatch).
const LYING_FRAME: usize = 64 * 1024;

/// Bao interleaved WIRE bytes for the whole of `payload` (content plus proof, ADR
/// 038) — the header-stripped stream a `cdn/client/v1` server emits. Lets a
/// hostile server serve the encoding of a DIFFERENT blob under the requested hash.
fn honest_bao_wire(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let hash = Hash::new(payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        payload,
        decdn_cache::range_pull::IROH_BLOCK_SIZE,
    );
    let aligned = decdn_cache::range_pull::align_range(0, 0, total_bytes)?;
    let combined = decdn_cache::range_pull::encode_verified_range(
        *hash.as_bytes(),
        &aligned,
        payload,
        bytes::Bytes::from(ob.data),
    )?;
    Ok(combined
        .get(8..)
        .ok_or_else(|| anyhow::anyhow!("combined encoding shorter than its header"))?
        .to_vec())
}

/// How a [`serve_lying_stream`] upstream misbehaves after signing a valid
/// `StreamResponse`.
#[derive(Clone, Copy)]
enum Lie {
    /// Stream the honest wire, then one extra full frame past the promised
    /// length — the OOM / overpay shape the client's overrun guard closes.
    OverSend,
    /// Stream the bao encoding of a different blob of the SAME length under the
    /// requested hash — a paid-but-corrupt delivery the client's verifier rejects.
    WrongBytes,
}

/// A raw `cdn/client/v1` upstream that accepts the request, signs a VALID
/// `StreamResponse` (so the buyer opens the stream and starts paying), then lies
/// on the byte stream per [`Lie`]. Models the hostile server the buyer's own
/// receive-side guards defend against — the counterpart to the payment-fault
/// tests, which model a hostile client.
async fn serve_lying_stream(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    served_wire: &[u8],
    advertised_total_bytes: u64,
    lie: Lie,
) -> anyhow::Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = match read_client_msg(&mut recv).await? {
        ClientMessage::StreamRequest(req) => req,
        other => anyhow::bail!("lying upstream: expected a StreamRequest, got {other:?}"),
    };

    let body = StreamResponseBody {
        hash: req.hash,
        ok: true,
        rate_per_mb: RATE_PER_MB,
        total_bytes: advertised_total_bytes,
        pool_id: req.pool_id,
        timestamp_us: req.timestamp_us,
    };
    let slash_sig = StreamSlashData::from_response_body(&body)
        .sign(eth.as_ref(), slash)
        .map_err(|e| anyhow::anyhow!("slash sign: {e}"))?
        .as_bytes()
        .to_vec();
    let resp = StreamResponse { body, slash_sig };
    write_frame(
        &mut send,
        &encode_stream_response(&resp, Some(&StreamResponseExt { error: None }))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;

    for chunk in served_wire.chunks(LYING_FRAME) {
        write_client_msg(
            &mut send,
            &ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?),
        )
        .await?;
    }

    match lie {
        Lie::OverSend => {
            // One extra full frame past the promised length trips the buyer's
            // `cumulative > expected_wire` overrun guard; it bails before any
            // `StreamEnd`, so nothing more is owed here.
            write_client_msg(
                &mut send,
                &ClientMessage::ChunkData(ChunkData::new(vec![0u8; LYING_FRAME])?),
            )
            .await?;
        }
        Lie::WrongBytes => {
            // The promised byte count is honest, so the buyer reads to the end,
            // pays the closing voucher, and only then verifies. Consume that
            // voucher and close cleanly so the failure is unambiguously the
            // integrity check, not a truncated stream.
            let _ = read_client_msg(&mut recv).await;
            write_client_msg(&mut send, &ClientMessage::StreamEnd).await?;
        }
    }
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Spawn a one-connection [`serve_lying_stream`] server.
fn spawn_lying_server(
    ep: Endpoint,
    eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    served_wire: Vec<u8>,
    advertised_total_bytes: u64,
    lie: Lie,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Ok(conn) = support::accept_one(&ep).await {
            let _ = serve_lying_stream(
                conn,
                &eth,
                &slash,
                &served_wire,
                advertised_total_bytes,
                lie,
            )
            .await;
        }
    })
}

/// The buyer rejects a server that streams more bytes than its signed
/// `StreamResponse` promised: the `cumulative > expected_wire` overrun guard bails
/// the fetch before the extra bytes can drive an OOM or an overpay (ADR 005
/// §`cdn/client/v1`).
#[tokio::test(flavor = "multi_thread")]
async fn client_rejects_a_server_that_oversends_past_the_promised_length() -> anyhow::Result<()> {
    // Under one interval, so the honest wire is a single closing-voucher span and
    // the extra frame is unambiguously past the promise.
    let payload = vec![0x2Du8; 256 * 1024];
    let hash = Hash::new(&payload);
    let wire = honest_bao_wire(&payload)?;
    let total_bytes = payload.len() as u64;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_lying_server(
        server_ep.clone(),
        Arc::clone(&server_eth),
        slash_domain(),
        wire,
        total_bytes,
        Lie::OverSend,
    );
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(
        &client_ep,
        Arc::new(PrivateKeySigner::random()),
        U256::from(10_000_000u64),
    );
    let err = stream_fetch(
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
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("an over-sending server must fail the fetch"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("more than"),
        "the fetch must fail on the overrun guard, got: {msg}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The buyer rejects hash-mismatched bytes: a server that serves the bao encoding
/// of a DIFFERENT blob under the requested hash passes the framing but fails
/// verification, so the fetch returns the typed [`HashMismatch`] and surfaces no
/// bytes (ADR 038 §Receive side).
#[tokio::test(flavor = "multi_thread")]
async fn client_rejects_hash_mismatched_bytes() -> anyhow::Result<()> {
    // Two distinct blobs of the SAME length: the wire lengths match, so the
    // buyer reads exactly the promised count and the only thing wrong is content.
    let wanted = vec![0x11u8; 256 * 1024];
    let served = vec![0x22u8; 256 * 1024];
    let wanted_hash = Hash::new(&wanted);
    anyhow::ensure!(Hash::new(&served) != wanted_hash, "fixtures must differ");
    let served_wire = honest_bao_wire(&served)?;
    let total_bytes = wanted.len() as u64;

    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_lying_server(
        server_ep.clone(),
        Arc::clone(&server_eth),
        slash_domain(),
        served_wire,
        total_bytes,
        Lie::WrongBytes,
    );
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let ctx = channel_context(
        &client_ep,
        Arc::new(PrivateKeySigner::random()),
        U256::from(10_000_000u64),
    );
    let err = stream_fetch(
        &client_ep,
        target,
        &ctx,
        &slash_domain(),
        server_eth.address(),
        *wanted_hash.as_bytes(),
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a hash-mismatched delivery must fail the fetch"))?;
    anyhow::ensure!(
        err.downcast_ref::<HashMismatch>().is_some(),
        "the fetch must fail with the typed HashMismatch, got: {err:#}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The request-read timeout tears down a stream whose opener goes silent: a peer
/// that opens a bidi stream and sends no `StreamRequest` is reset once
/// `REQUEST_READ_TIMEOUT` elapses, with the no-error code (a stalled peer is not
/// a protocol fault) — the node never blocks a serve slot on a silent opener.
#[tokio::test(flavor = "multi_thread")]
async fn request_read_timeout_resets_a_silent_opener() -> anyhow::Result<()> {
    let payload = vec![0x61u8; 64 * 1024];
    let (cache, _hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let (store, _signer, _deposit) = seeded_store()?;

    let (target, _server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    // Open a stream and send only a frame LENGTH prefix, withholding the body: the
    // stream reaches the server (an unwritten `open_bi` never does), which parks in
    // `read_first_message`'s framed read and must give up at the request-read
    // timeout (5s). `send` is held for the whole wait so the client does not FIN
    // and end the read early — the reset must be the server's timeout, not our EOF.
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    send.write_all(&[64u8])
        .await
        .map_err(|e| anyhow::anyhow!("write frame len: {e}"))?;

    // The parked read is reset (or the connection closed) once the timeout fires;
    // either way no frame is ever readable. A clean empty EOF would mean the peer
    // FIN'd — impossible here, `send` is still open — so it is not accepted.
    let ended = tokio::time::timeout(Duration::from_secs(15), read_frame(&mut recv)).await;
    match ended {
        Ok(Err(_)) => {}
        Ok(Ok(frame)) => {
            anyhow::bail!(
                "server answered a partial request with a {}-byte frame",
                frame.len()
            )
        }
        Err(_elapsed) => {
            anyhow::bail!("server never reset a silent opener within the request-read timeout")
        }
    }
    drop(send);

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The voucher-read timeout tears down a parked paid delivery: a payer that reads
/// the whole blob and then withholds its closing voucher is cut off once
/// `VOUCHER_READ_TIMEOUT` elapses (the stream resets with no `StreamEnd`), and no
/// watermark advances — a silent payer cannot pin the serve loop indefinitely.
#[tokio::test(flavor = "multi_thread")]
async fn voucher_read_timeout_ends_a_parked_delivery() -> anyhow::Result<()> {
    // Under one credit-window floor, so the delivery parks at a single closing
    // voucher — the exact point `commit_one_proof` blocks under the timeout.
    let payload = vec![0x62u8; 64 * 1024];
    // Idle window well past the 10s voucher-read timeout the test is waiting on,
    // so the app-layer idle reaper never fires first.
    let fx = idle_fixture(&payload, Duration::from_secs(45)).await?;
    let blob = fx.blob(0)?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let conn = client_ep
        .connect(fx.target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let client_node_id = B256::from(*client_ep.id().as_bytes());
    let ext = binding_ext(&fx.client_signer, client_node_id)?;
    let mut stalled = stall_delivery_at_closing_voucher(
        &conn,
        *blob.hash.as_bytes(),
        blob.wire_bytes,
        Some(&ext),
    )
    .await?;

    // Withhold the voucher. The server is parked in `commit_one_proof`'s bounded
    // read and must give up at the voucher-read timeout (10s), ending the stream
    // with no `StreamEnd` — whether by a reset or a clean close, the payment never
    // completes, so the next read yields an error rather than a `StreamEnd`.
    let ended =
        tokio::time::timeout(Duration::from_secs(20), read_client_msg(&mut stalled.recv)).await;
    match ended {
        Ok(Err(_)) => {}
        Ok(Ok(ClientMessage::StreamEnd)) => {
            anyhow::bail!("the delivery must not complete without a paid closing voucher")
        }
        Ok(Ok(other)) => anyhow::bail!("expected the stream to end unpaid, got {other:?}"),
        Err(_elapsed) => {
            anyhow::bail!("server never ended the parked delivery within the voucher-read timeout")
        }
    }

    // No voucher was ever accepted, so the lane watermark stayed at zero.
    let persisted = fx.store.load_all()?;
    if let Some(lane) = persisted.first() {
        anyhow::ensure!(
            lane.last_bytes_delivered() == U256::ZERO,
            "a timed-out delivery must advance no watermark, got {}",
            lane.last_bytes_delivered()
        );
    }

    shutdown([fx.server_task], [&client_ep, &fx.server_ep]).await?;
    Ok(())
}
