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
//! Still uncovered (need a hostile client that reimplements the receive loop,
//! tracked as follow-ups): a mid-stream underpaying voucher → stream fails; a
//! `BadSignature`/`StaleNonce` rejection *after* an accepted voucher; the
//! per-connection stream-cap reset-without-signing; a server over-sending or
//! delivering hash-mismatched bytes; and the request/voucher read timeouts.

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_cache::CacheEngine;
use decdn_incentive::{
    ChannelState, ChannelStateStore, EPHEMERAL_BINDING_NONCE, MemoryChannelStateStore,
    bind_node_id_domain, binding_signing_hash, slash_judge_domain, voucher_domain,
};
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::client::ClientHandler;
use decdn_node::metrics::Metrics;
use decdn_node::region_accounting::{RegionAccountant, RegionResolver, UNKNOWN_REGION};
use decdn_protocol::client::{ClientBinding, ClientMessage, StreamRequest, StreamRequestExt};
use decdn_protocol::{ALPN_CLIENT, decode_message, encode_stream_request, read_frame, write_frame};
use iroh::{Endpoint, EndpointAddr};

mod support;
use decdn_node::receipt_log::{DownloadReceipt, spawn_receipt_writer};
use support::{
    BlockingReceiptLog, FailingReceiptLog, HandlerDomains, VecReceiptLog, build_handler_full,
    build_handler_full_with_receipts, build_handler_full_with_sink, cache_with_blob, empty_cache,
    fresh_key, local_endpoint, permissive_limiter, spawn_server,
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
        only.last_nonce() == U256::from(2u64),
        "nonce: {}",
        only.last_nonce()
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(payload.len()),
        "bytes_delivered: {}",
        only.last_bytes_delivered()
    );
    anyhow::ensure!(only.last_amount() > U256::ZERO, "amount must be non-zero");

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

    // Share one `Arc<VoucherActivity>` with the handler — the same wiring
    // `runtime::run` performs (one Arc cloned into the handler and the admin
    // surface). Before any accept the channel is unknown to the clock.
    let activity = Arc::new(VoucherActivity::new());
    handler.attach_voucher_activity(Arc::clone(&activity));
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

    // The client endpoint's key is what the handler sees as `client_node_id`.
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let accountant = Arc::new(RegionAccountant::new(Arc::new(OneRegion {
        node_id: *client_id.as_bytes(),
        region: "DE".to_string(),
    })));
    handler.attach_region_accountant(Arc::clone(&accountant));
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
    anyhow::ensure!(
        de.bytes_out == payload.len() as u64,
        "DE bytes_out = {}, expected {}",
        de.bytes_out,
        payload.len()
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
    anyhow::ensure!(
        total == payload.len() as u64,
        "receipt sizes sum to {total}, expected {}",
        payload.len()
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
    anyhow::ensure!(
        total == payload.len() as u64,
        "drained receipt sizes sum to {total}, expected {} ({} receipts)",
        payload.len(),
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
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(payload.len()),
        "channel state must still advance: bytes_delivered={}",
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
        only.last_bytes_delivered() == U256::from(suffix.len()),
        "bytes_delivered should be the suffix length, got {}",
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
    let (target, _server_eth, server_ep, server_task) =
        spawn_handler_server(cache, store, RATE_PER_MB, 0, 16).await?;

    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        channel_id: channel_id().into(),
        byte_offset: 0,
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
    let mut expired = ChannelState::new(channel_id(), client_signer.address(), TOKEN, deposit);
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
    server_task.await?;
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
        only.last_nonce() == U256::from(1u64),
        "nonce: {}",
        only.last_nonce()
    );
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(payload.len()),
        "bytes_delivered: {}",
        only.last_bytes_delivered()
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
        TOKEN,
        U256::from(10_000_000u64),
        U256::from(4_321u64),
        U256::from(5u64),
        U256::from(2_048u64),
        Some([0x11; 65]),
        0,
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
    let handler = build_handler_full(
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
    )?;
    // Arm pull-through, as the runtime does when the feature is enabled.
    handler.attach_pull_through(std::time::Duration::from_secs(10));
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // A hash the node does not have → a miss that would trigger the pull.
    let miss_hash = [0xEEu8; 32];
    let req = StreamRequest {
        hash: miss_hash,
        channel_id: channel_id().into(),
        byte_offset: 0,
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

// ===========================================================================
// #859 — handler-level outer-deadline regression. `ClientHandler::try_pull_through`
// wraps the whole pull in ONE `tokio::time::timeout`. Before #859 that outer
// deadline was set EQUAL to the per-candidate budget (`node_pull_timeout_sec`),
// so a pull needing more than one per-candidate budget's wall-clock (e.g.
// candidate #1 stalls a full budget, then #2 delivers) was cancelled before it
// could finish. The derived `outer_pull_deadline` (N×per + slack) must
// accommodate it. A single slow origin taking longer than one per-candidate
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
/// background fill is attached, so this isolates the foreground outer-deadline
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
    let handler = build_handler_full(
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
    )?;
    handler.attach_pull_through(outer_deadline);
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let req = StreamRequest {
        hash: *want.as_bytes(),
        channel_id: channel_id().into(),
        byte_offset: 0,
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
/// the suite that fails if `attach_pull_through` is re-wired to the per-candidate
/// value (re-introducing #859).
#[tokio::test(flavor = "multi_thread")]
async fn pull_through_outer_deadline_accommodates_a_slow_pull() -> anyhow::Result<()> {
    let per = Duration::from_secs(1);
    let slow = Duration::from_millis(1500); // > per, well under outer_pull_deadline(per)

    // Pre-#859 wiring: outer == per_candidate cancels the slow pull → store empty.
    anyhow::ensure!(
        !pull_through_fills_under_deadline(per, slow).await?,
        "an outer deadline equal to the per-candidate budget must abandon the slow pull (the #859 bug)"
    );
    // Fixed wiring: the derived outer deadline accommodates it → store filled.
    anyhow::ensure!(
        pull_through_fills_under_deadline(decdn_node::selection::outer_pull_deadline(per), slow)
            .await?,
        "the derived outer deadline must let a pull exceeding one per-candidate budget complete"
    );
    Ok(())
}
