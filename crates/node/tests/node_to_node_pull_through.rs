//! Two-hop node-to-node paid pull-through over `cdn/client/v1` (#746).
//!
//! The single-hop loopback suite (`client_loopback.rs`) only exercises
//! client → one node. This binary covers the multi-hop revenue/correctness
//! path: a blob originates at an upstream node A, is **paid-pulled** to a mid
//! node B over `cdn/client/v1`, and is then **paid-served** by B to a leaf
//! client over the same protocol. Both hops are real in-process iroh exchanges
//! on localhost with off-chain vouchers — no anvil, no RPC, no contract.
//!
//! Each hop reuses the complete protocol halves shipped in PR #733:
//! [`ClientHandler`] (server) and [`stream_fetch`] (requester). The test
//! asserts the bytes survive both paid hops intact (hash-verified inside
//! `stream_fetch`) and that *both* off-chain payment channels advance
//! (nonce / bytes-delivered / cumulative amount).
//!
//! **Boundary (deliberately not covered here).** The production runtime does
//! NOT yet auto-wire this flow. The cache-engine hook that, on a miss, would
//! discover a provider, open the upstream channel, pull, and populate the cache
//! is deferred on provider discovery (ADR 001/022) — `runtime/mod.rs` binds the
//! bootstrapped `BuyerChannelService` to `_buyer_channel_service` and notes the
//! same, with no tracking issue for the hook itself. There is also no
//! `NodeOrigin` impl. This test therefore performs those steps manually: hop 1
//! opens the channel and pulls, and step 2 populates B's cache. Of those, the
//! cache population is the one with no public API at all — `CacheEngine` only
//! ingests via origins — so step 2 stands one up: `cache_with_blob(&pulled)`
//! builds B's cache by round-tripping the paid-for bytes through a throwaway
//! filesystem origin (see `support::cache_with_blob`). This test composes the
//! building blocks that exist today; it does not assert the not-yet-built
//! runtime orchestration.

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::CacheEngine;
use decdn_incentive::{
    ChannelState, ChannelStateStore, MemoryChannelStateStore, StreamSlashData, bind_node_id_domain,
    slash_judge_domain, voucher_domain,
};
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::metrics::Metrics;
use decdn_protocol::client::{
    ChunkData, ClientMessage, StreamRequest, StreamResponse, StreamResponseBody,
};
use decdn_protocol::{ALPN_CLIENT, encode_stream_request, write_frame};
use iroh::{Endpoint, EndpointAddr};

mod support;
use support::{
    HandlerDomains, accept_one, build_handler_full, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter, read_client_msg, spawn_server, write_client_msg,
};

const CHAIN_ID: u64 = 421_614;
const TOKEN: Address = Address::repeat_byte(0x22);
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// 1.5 MiB → crosses one 1-MiB voucher interval plus a closing voucher, so each
/// hop's channel advances to nonce 2 (mirrors `client_loopback.rs`).
const PAYLOAD_LEN: usize = 1_572_864;
/// Distinct per-hop rates: A charges B `RATE_A`, B charges the client `RATE_B`.
/// The two channels' cumulative `amount`s must reflect their *own* rate, proving
/// pricing is per-server (no cross-talk between B's buyer and seller roles).
const RATE_A: u64 = 10;
const RATE_B: u64 = 13;

/// Cumulative voucher amount the requester pays for a `PAYLOAD_LEN` blob at
/// `rate` per MiB. ADR 038: vouchers meter the bao **wire** byte stream (content
/// plus interleaved Merkle proof), so the amount is computed over the whole-blob
/// wire size, not the content length. The stream pays `rate` per fully crossed
/// 1-MiB interval plus a closing voucher of `ceil(remainder * rate / MiB)` for
/// the trailing wire bytes, mirroring the requester's per-voucher
/// `ceil(bytes_delta * rate / MiB)` arithmetic in `client_requester::send_voucher`.
fn expected_amount(rate: u64) -> U256 {
    const MIB: u64 = 1024 * 1024;
    let wire = support::bao_wire_len_whole(PAYLOAD_LEN as u64);
    let full_intervals = wire / MIB;
    let remainder = wire % MIB;
    let mut amount = full_intervals.saturating_mul(rate);
    if remainder > 0 {
        amount = amount.saturating_add(remainder.saturating_mul(rate).div_ceil(MIB));
    }
    U256::from(amount)
}

const fn slash_verifying() -> Address {
    Address::repeat_byte(0x11)
}

fn slash_domain() -> Eip712Domain {
    slash_judge_domain(CHAIN_ID, slash_verifying())
}

/// The fixed EIP-712 domains shared by both hops' handlers. The hops are
/// distinguished by per-node identities and channel ids, not by domain.
fn hop_domains() -> HandlerDomains {
    HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_domain(CHAIN_ID, Address::repeat_byte(0x34)),
        binding: bind_node_id_domain(CHAIN_ID, Address::repeat_byte(0x99)),
    }
}

/// A fresh-channel [`ChannelContext`] (all `prior_*` at zero) for `signer`
/// paying on `channel_id`.
fn fresh_context(channel_id: B256, signer: Arc<PrivateKeySigner>) -> ChannelContext {
    ChannelContext {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        client_signer: signer,
        voucher_domain: hop_domains().voucher,
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    }
}

/// Assert that `store` holds **exactly** the channel `channel_id` and that it
/// advanced as expected after one full delivery: two vouchers (interval +
/// closing), full byte count, and the `expected_amount(rate)` cumulative amount.
///
/// The exactness (`len() == 1` + matching id + rate-derived amount) is what
/// makes this a real two-hop assertion rather than two single hops: a bug that
/// charged the wrong channel, applied the wrong rate, or leaked B's buyer-role
/// state into its seller store would change the count or the amount.
fn assert_channel_advanced(
    store: &MemoryChannelStateStore,
    hop: &str,
    channel_id: B256,
    rate: u64,
) -> anyhow::Result<()> {
    let persisted = store.load_all()?;
    anyhow::ensure!(
        persisted.len() == 1,
        "{hop}: expected exactly one channel, got {}",
        persisted.len()
    );
    let only = store
        .get(channel_id)?
        .ok_or_else(|| anyhow::anyhow!("{hop}: channel {channel_id} not in store"))?;
    anyhow::ensure!(
        only.last_nonce() == U256::from(2u64),
        "{hop}: nonce {}",
        only.last_nonce()
    );
    // ADR 038: metered quantity is bao wire bytes (content + interleaved Merkle
    // proof), not the content length, so the recorded watermark is the whole-blob
    // wire size — same for every hop, each metered in wire bytes.
    let wire_bytes = support::bao_wire_len_whole(PAYLOAD_LEN as u64);
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(wire_bytes),
        "{hop}: bytes_delivered {} != expected wire {wire_bytes}",
        only.last_bytes_delivered()
    );
    anyhow::ensure!(
        only.last_amount() == expected_amount(rate),
        "{hop}: amount {} != expected {}",
        only.last_amount(),
        expected_amount(rate)
    );
    Ok(())
}

/// Build a `ClientHandler` over `cache`/`store` with the shared hop domains, an
/// unlimited blob-size gate, and a 16-stream per-connection cap.
fn build_server(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
) -> anyhow::Result<Arc<decdn_node::handlers::client::ClientHandler>> {
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    build_handler_full(
        server_id,
        server_eth,
        &metrics,
        limiter,
        cache,
        store,
        rate,
        &hop_domains(),
        0,  // max_blob_size_bytes (0 == unlimited)
        16, // max_concurrent_streams
    )
}

/// End-to-end two-hop paid pull-through: A → B (paid pull), then B → client
/// (paid serve). Bytes survive both hops and both off-chain channels advance.
#[tokio::test(flavor = "multi_thread")]
async fn node_to_node_pull_through_two_hops() -> anyhow::Result<()> {
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let hash = decdn_cache::Hash::new(&payload);

    // --- Node A: upstream holder + server ---------------------------------
    let (cache_a, hash_a, _cache_a_tmp) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());

    // Node B's buyer identity (B pays A on the A↔B channel).
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let a_channel_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        a_channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;

    let handler_a = build_server(a_id, &a_eth, cache_a, store_a.clone(), RATE_A)?;
    let (ep_a, addr_a) = local_endpoint(a_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_server(ep_a.clone(), handler_a);

    // --- Hop 1: B pulls the blob from A, paying vouchers ------------------
    // B's single endpoint serves both roles: it dials out to A here, and below
    // it accepts the client's connection. Only the dial-out is needed now.
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;

    let target_a = EndpointAddr::new(a_id).with_ip_addr(addr_a);
    let ctx_b_to_a = fresh_context(a_channel_id, Arc::clone(&b_buyer));
    let pulled = stream_fetch(
        &ep_b,
        target_a,
        &ctx_b_to_a,
        &slash_domain(),
        a_eth.address(),
        *hash.as_bytes(),
        0,
        0x00b0_0a01,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(
        pulled.as_ref() == payload.as_slice(),
        "hop 1 bytes mismatch"
    );
    assert_channel_advanced(&store_a, "A<->B", a_channel_id, RATE_A)?;

    // --- Step 2: build B's cache from the pulled bytes -------------------
    // Stand-in for the deferred runtime wiring: a real node's `NodeOrigin` would
    // populate the cache during the miss. `CacheEngine` has no public ingest
    // API (only origin pull-through), so `cache_with_blob` builds B's cache by
    // round-tripping the paid-for bytes through a throwaway filesystem origin.
    let (cache_b, hash_b, _cache_b_tmp) = cache_with_blob(&pulled).await?;
    anyhow::ensure!(hash_b == hash, "ingested hash mismatch");

    // --- Node B: server toward the client --------------------------------
    let b_eth = Arc::new(PrivateKeySigner::random());
    let client_signer = Arc::new(PrivateKeySigner::random());
    let b_channel_id = B256::repeat_byte(0xB2);
    let store_b = Arc::new(MemoryChannelStateStore::new());
    store_b.record(&ChannelState::new(
        b_channel_id,
        client_signer.address(),
        client_signer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;

    let handler_b = build_server(b_id, &b_eth, cache_b, store_b.clone(), RATE_B)?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    // --- Hop 2: client pulls the blob from B, paying vouchers ------------
    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    let ctx_client_to_b = fresh_context(b_channel_id, Arc::clone(&client_signer));
    let got = stream_fetch(
        &client_ep,
        target_b,
        &ctx_client_to_b,
        &slash_domain(),
        b_eth.address(),
        *hash.as_bytes(),
        0,
        0x00c1_0a02,
        Duration::from_secs(20),
    )
    .await?;
    // End-to-end: the blob originated at A and survived both paid hops intact.
    anyhow::ensure!(got.as_ref() == payload.as_slice(), "hop 2 bytes mismatch");
    assert_channel_advanced(&store_b, "B<->client", b_channel_id, RATE_B)?;

    // Cross-hop isolation: serving the client (hop 2) must not have touched the
    // A<->B channel. Re-assert A's channel is unchanged at hop-1's accounting.
    assert_channel_advanced(&store_a, "A<->B after hop 2", a_channel_id, RATE_A)?;

    client_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    // Propagate JoinResult so a panic in either accept loop fails the test
    // rather than being silently swallowed. After `close()` the loops exit
    // cleanly, so this only surfaces genuine background-task panics.
    task_b.await?;
    task_a.await?;
    Ok(())
}

// ===========================================================================
// Sad paths (#746)
//
// The happy path above proves the multi-hop flow works when every party is
// honest and reachable. The triage for #746 also called out three failure
// modes the revenue/correctness path must survive cleanly. Each is a *node-to-
// node* fault: the downstream node is the `cdn/client/v1` requester
// (`stream_fetch`) pulling from an upstream that is unreachable, abandoned
// mid-stream by its own client, or dishonest about the bytes it serves.
//
// The raw framed-message primitives these tests hand-roll (`read_client_msg`,
// `write_client_msg`, `accept_one`) live in `support` so the other client
// binaries can share them; only the test-specific `lying_upstream` is local.
// ===========================================================================

/// Sad path: opening the upstream delivery channel fails (#746).
///
/// We model an unusable upstream as one that is reachable at the transport but
/// refuses to serve: its accept loop takes the QUIC connection and immediately
/// closes it, so the requester's connect or first read fails fast (which exact
/// stage loses the race is timing-dependent; the test pins only that it fails
/// *fast*, not at the deadline). This is a deterministic, millisecond-fast
/// stand-in for the "unreachable / channel open
/// fails" family — a genuinely dead UDP port works too, but whose failure mode
/// (ICMP "port unreachable" vs. a silent multi-second handshake timeout) is
/// environment-dependent and would make the test slow and flaky. The property
/// under test is the requester contract: a failed upstream pull surfaces as a
/// clean `Err` — never a panic, a hang past the deadline, or a partial/empty
/// `Ok` — so the downstream node can in turn return an error to its own client.
/// No voucher is ever signed because no delivery proceeds.
#[tokio::test(flavor = "multi_thread")]
async fn upstream_channel_open_failure_pull_fails_cleanly() -> anyhow::Result<()> {
    // Upstream A speaks `cdn/client/v1` but hangs up on every connection instead
    // of serving — the requester sees the channel collapse before any bytes.
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let (ep_a, addr_a) = local_endpoint(a_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let refuser = {
        let ep_a = ep_a.clone();
        tokio::spawn(async move {
            while let Ok(conn) = accept_one(&ep_a).await {
                conn.close(0u32.into(), b"refused");
            }
        })
    };

    // Downstream B dials A for a blob it cannot get.
    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target_a = EndpointAddr::new(a_id).with_ip_addr(addr_a);
    let ctx = fresh_context(
        B256::repeat_byte(0xA1),
        Arc::new(PrivateKeySigner::random()),
    );

    let result = stream_fetch(
        &ep_b,
        target_a,
        &ctx,
        &slash_domain(),
        Address::repeat_byte(0x55), // expected upstream signer (never reached)
        [0x42u8; 32],
        0,
        0x00de_ad01,
        Duration::from_secs(10),
    )
    .await;

    let err = result.err().ok_or_else(|| {
        anyhow::anyhow!("a pull from a refusing upstream must fail, not return bytes")
    })?;
    // Pin the failure to a *fast* transport collapse (connect / open_bi / first
    // read), not the deadline backstop: an error carrying "timed out" would mean
    // the requester hung to the 10s limit instead of surfacing the refused
    // channel — the "never a hang past the deadline" half of the contract, and
    // the discriminator that stops this passing for the wrong reason.
    anyhow::ensure!(
        !err.to_string().contains("timed out"),
        "pull should fail fast on the collapsed channel, not hang to the deadline: {err}"
    );

    ep_b.close().await;
    ep_a.close().await; // ends the refuser's accept loop (its next accept yields None)
    // Join rather than abort, so a panic inside the fake upstream surfaces here
    // instead of being silently dropped — matching how the happy path joins its
    // server tasks.
    refuser.await?;
    Ok(())
}

/// Sad path: the client disconnects mid-stream (#746).
///
/// A downstream node B (the real [`ClientHandler`]) is paid-serving a 1.5 MiB
/// blob. Its client reads the signed response and one chunk, then aborts the
/// connection before paying any voucher. Two properties must hold:
///
/// 1. The aborted pull advances **nothing** — B accepts no voucher, so the
///    channel stays at nonce 0. No payment is fabricated for un-acked bytes and
///    no half-open delivery is committed to the store.
/// 2. The channel is **not wedged**: a subsequent honest pull on the *same*
///    channel completes and advances it normally, proving the mid-stream abort
///    left behind no poisoned per-channel lock or dangling delivery state (the
///    serial accept loop, in particular, must recover to serve the next client).
///
/// [`ClientHandler`]: decdn_node::handlers::client::ClientHandler
#[tokio::test(flavor = "multi_thread")]
async fn client_disconnect_mid_stream_leaves_channel_reusable() -> anyhow::Result<()> {
    let payload = vec![0x6Du8; PAYLOAD_LEN];
    let hash = decdn_cache::Hash::new(&payload);
    let (cache, hash_b, _cache_tmp) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_b == hash, "fixture hash mismatch");

    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let b_eth = Arc::new(PrivateKeySigner::random());
    let client_signer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0xC1);
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id,
        client_signer.address(),
        client_signer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;

    let handler = build_server(b_id, &b_eth, cache, store.clone(), RATE_B)?;
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let task_b = spawn_server(ep_b.clone(), handler);

    // --- Abort: request, read the response + one chunk, then hang up ---------
    {
        let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
        let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
        let conn = client_ep
            .connect(target_b, ALPN_CLIENT)
            .await
            .map_err(|e| anyhow::anyhow!("client connect: {e}"))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = StreamRequest {
            hash: *hash.as_bytes(),
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            channel_id: channel_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: 0x00c0_ffee,
        };
        let req_bytes = encode_stream_request(&req, None)
            .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
        write_frame(&mut send, &req_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("write request: {e}"))?;

        match read_client_msg(&mut recv).await? {
            ClientMessage::StreamResponse(r) => {
                anyhow::ensure!(r.body.ok, "expected an ok response, got {:?}", r.error);
            }
            _ => anyhow::bail!("expected a StreamResponse first"),
        }
        // Read at least one chunk so the abort is genuinely *mid*-stream, then
        // drop without paying. The explicit close hands B a CONNECTION_CLOSE so
        // its voucher read unblocks at once (vs. waiting on an idle timeout),
        // freeing the serial accept loop to serve the honest pull below.
        match read_client_msg(&mut recv).await? {
            ClientMessage::ChunkData(_) => {}
            _ => anyhow::bail!("expected a ChunkData mid-stream"),
        }
        conn.close(0u32.into(), b"client-abort");
        client_ep.close().await;
    }

    // Property 1: no voucher accepted — the channel never advanced. Both nonce
    // and bytes-delivered must still read zero (their initial state); checking
    // bytes too rules out any partial accounting committed for the unpaid chunk.
    let after_abort = store
        .get(channel_id)?
        .ok_or_else(|| anyhow::anyhow!("channel vanished after the abort"))?;
    anyhow::ensure!(
        after_abort.last_nonce() == U256::ZERO,
        "an aborted pull advanced the channel to nonce {}",
        after_abort.last_nonce()
    );
    anyhow::ensure!(
        after_abort.last_bytes_delivered() == U256::ZERO,
        "an aborted pull committed {} bytes of accounting",
        after_abort.last_bytes_delivered()
    );

    // Property 2: the same channel still serves. A fresh-context honest pull
    // completes and advances it (the abort left prior state at zero).
    let (honest_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    let ctx = fresh_context(channel_id, Arc::clone(&client_signer));
    let got = stream_fetch(
        &honest_ep,
        target_b,
        &ctx,
        &slash_domain(),
        b_eth.address(),
        *hash.as_bytes(),
        0,
        0x00c0_0d02,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "honest pull bytes mismatch"
    );
    assert_channel_advanced(&store, "reused after abort", channel_id, RATE_B)?;

    honest_ep.close().await;
    ep_b.close().await;
    task_b.await?;
    Ok(())
}

/// A protocol-correct but **dishonest** `cdn/client/v1` upstream: it signs a
/// valid response for the requested hash, streams `served` (whose hash differs),
/// reads and acks the closing voucher, and ends the stream cleanly — yet the
/// bytes are wrong. This drives the requester all the way to its whole-blob
/// integrity check. Handles exactly one connection, then returns.
async fn lying_upstream(
    ep: &Endpoint,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    served: &[u8],
    rate_per_mb: u64,
) -> anyhow::Result<()> {
    let conn = accept_one(ep).await?;
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;

    // Echo the request's correlated fields back so the response validates
    // (ADR 005): only the *bytes* are dishonest, not the framing.
    let ClientMessage::StreamRequest(req) = read_client_msg(&mut recv).await? else {
        anyhow::bail!("lying upstream: expected a StreamRequest");
    };
    let body = StreamResponseBody {
        hash: req.hash,
        ok: true,
        rate_per_mb,
        total_bytes: u64::try_from(served.len())
            .map_err(|_| anyhow::anyhow!("served len overflows u64"))?,
        channel_id: req.channel_id,
        timestamp_us: req.timestamp_us,
        redirect: None,
    };
    let slash_sig = StreamSlashData::from_response_body(&body)
        .sign(eth.as_ref(), slash)
        .map_err(|e| anyhow::anyhow!("slash sign: {e}"))?
        .as_bytes()
        .to_vec();
    let resp = StreamResponse {
        body,
        error: None,
        voucher_interval_mb: Some(1),
        slash_sig,
    };
    write_client_msg(&mut send, &ClientMessage::StreamResponse(resp)).await?;

    // The wrong bytes, streamed in `CHUNK_SIZE` chunks like the real handler (the
    // requester rejects any chunk over the ceiling). `served` stays under one
    // voucher interval, so exactly one closing voucher flows.
    for chunk in served.chunks(decdn_protocol::CHUNK_SIZE) {
        write_client_msg(
            &mut send,
            &ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?),
        )
        .await?;
    }

    // Accept the closing voucher the requester pays for the bytes it received,
    // so it proceeds to the integrity check (the unit under test) rather than
    // bailing early on a rejected voucher.
    match read_client_msg(&mut recv).await? {
        ClientMessage::Voucher(_) => {
            write_client_msg(&mut send, &ClientMessage::VoucherAck).await?;
        }
        _ => anyhow::bail!("lying upstream: expected a Voucher"),
    }
    write_client_msg(&mut send, &ClientMessage::StreamEnd).await?;
    let _ = send.finish();
    // Hold the connection open until the requester has read `StreamEnd` and
    // closed (it closes with `verify-failed` once a bao chunk group fails to
    // verify against the content root). Returning here would drop `conn` and
    // abort the still-in-flight `StreamEnd` before it lands, surfacing a spurious
    // "connection lost" instead.
    conn.closed().await;
    Ok(())
}

/// Sad path: the upstream returns bytes that don't match the requested hash
/// (#746).
///
/// Content is BLAKE3-addressed and the requester verifies every bao chunk group
/// against the content root (ADR 038) — but that defense had no node-to-node
/// test. Here a malicious / buggy upstream plays the protocol perfectly (valid
/// signed response, a paid and
/// ack'd voucher, a clean `StreamEnd`) while serving content that hashes to the
/// wrong value. The downstream requester MUST reject the delivery and return an
/// `Err`, never surfacing the corrupt bytes to its caller. This guards the
/// content-addressing invariant across a *paid* hop: a peer cannot substitute
/// content for a hash, even after being paid for it.
#[tokio::test(flavor = "multi_thread")]
async fn upstream_hash_mismatch_is_rejected() -> anyhow::Result<()> {
    // What the downstream asks for...
    let honest = vec![0x11u8; 4096];
    let requested_hash = decdn_cache::Hash::new(&honest);
    // ...vs. what the lying upstream actually serves (different content, sized
    // under one voucher interval).
    let served = vec![0x22u8; 2048];
    anyhow::ensure!(
        decdn_cache::Hash::new(&served) != requested_hash,
        "the two fixture payloads must differ"
    );

    let upstream_eth = Arc::new(PrivateKeySigner::random());
    let up_sk = fresh_key();
    let up_id = up_sk.public();
    let (ep_up, addr_up) = local_endpoint(up_sk, vec![ALPN_CLIENT.to_vec()]).await?;

    let liar = {
        let ep_up = ep_up.clone();
        let eth = Arc::clone(&upstream_eth);
        let slash = slash_domain();
        let served = served.clone();
        tokio::spawn(async move { lying_upstream(&ep_up, &eth, &slash, &served, RATE_A).await })
    };

    // Downstream B pulls the (honest) hash; the bytes that arrive are wrong.
    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(up_id).with_ip_addr(addr_up);
    let ctx = fresh_context(
        B256::repeat_byte(0xD1),
        Arc::new(PrivateKeySigner::random()),
    );
    let result = stream_fetch(
        &ep_b,
        target,
        &ctx,
        &slash_domain(),
        upstream_eth.address(),
        *requested_hash.as_bytes(),
        0,
        0x00d1_0a01,
        Duration::from_secs(20),
    )
    .await;

    let err = result.err().ok_or_else(|| {
        anyhow::anyhow!("a delivery that fails bao verification must be rejected")
    })?;
    anyhow::ensure!(
        err.to_string().contains("do not match requested hash"),
        "error should be the integrity check, got: {err}"
    );

    ep_b.close().await;
    // The liar reached `StreamEnd` before the requester bailed; surface any panic
    // or protocol error it hit, bounded so a hang fails loudly rather than stalls.
    match tokio::time::timeout(Duration::from_secs(5), liar).await {
        Ok(joined) => {
            joined.map_err(|e| anyhow::anyhow!("lying upstream task panicked: {e}"))??;
        }
        Err(_) => anyhow::bail!("lying upstream did not finish in time"),
    }
    ep_up.close().await;
    Ok(())
}
