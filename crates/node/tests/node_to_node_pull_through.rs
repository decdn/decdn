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
//! `stream_fetch`) and that *both* off-chain pool lanes advance
//! (bytes-delivered / cumulative amount).
//!
//! **Boundary (deliberately not covered here).** The runtime orchestration is
//! out of scope for this file. The cache-engine hook that, on a miss, discovers
//! a provider, opens the upstream channel, pulls, and populates the cache is
//! `decdn_node::node_origin::NodeOrigin`, which `runtime/mod.rs` constructs
//! and provisions when `cache.node_to_node_pull_through_enabled` is set;
//! `node_origin_pull.rs` is the suite that covers it. This file instead drives
//! the two protocol hops by hand: hop 1 opens the channel and pulls, and step 2
//! populates B's cache. Of those, the cache population is the awkward one: the
//! engine's public ingest paths are origin pull-through and the serve-miss pull
//! leg (`CacheEngine::claim_fill`), and the latter wants a bao verified stream
//! (ADR 038 framing) rather than the plaintext `stream_fetch` hands back — so
//! step 2 stands an origin up instead.
//! `cache_with_blob(&pulled)` builds B's cache by round-tripping the paid-for
//! bytes through a throwaway filesystem origin (see `support::cache_with_blob`).
//! Keeping the orchestration out means a failure here points at the wire halves,
//! not at discovery, reputation, or deadline policy.

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::CacheEngine;
use decdn_client::{PoolContext, stream_fetch};
use decdn_incentive::{
    EPHEMERAL_BINDING_NONCE, LaneKey, LaneState, MemoryPoolStateStore, PoolStateStore,
    StreamSlashData, bind_node_id_domain, binding_signing_hash, slash_judge_domain, voucher_domain,
};
use decdn_node::metrics::Metrics;
use decdn_protocol::client::{
    ChunkData, ClientBinding, ClientMessage, StreamRequest, StreamRequestExt, StreamResponse,
    StreamResponseBody,
};
use decdn_protocol::{ALPN_CLIENT, encode_stream_request, write_frame};
use iroh::endpoint::{Connection, ConnectionError};
use iroh::{Endpoint, EndpointAddr};

mod support;
use support::{
    HandlerDomains, accept_one, build_handler_full, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter, read_client_msg, shutdown, spawn_server, write_client_msg,
};

const CHAIN_ID: u64 = 421_614;
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// 1.5 MiB → crosses one 1 MiB chunk plus a closing voucher, so each
/// hop's lane advances through two vouchers (mirrors `client_loopback.rs`).
const PAYLOAD_LEN: usize = 1_572_864;
/// Distinct per-hop rates: A charges B `RATE_A`, B charges the client `RATE_B`.
/// The two lanes' cumulative `amount`s must reflect their *own* rate, proving
/// pricing is per-server (no cross-talk between B's buyer and seller roles).
const RATE_A: u64 = 10;
const RATE_B: u64 = 13;

/// Cumulative voucher amount the requester pays for a `PAYLOAD_LEN` blob at
/// `rate` per MiB. ADR 038: vouchers meter the bao **wire** byte stream (content
/// plus interleaved Merkle proof), so the amount is computed over the whole-blob
/// wire size, not the content length. The stream pays `rate` per fully crossed
/// 1-MiB interval plus a closing voucher of `ceil(remainder * rate / MiB)` for
/// the trailing wire bytes, mirroring the requester's per-voucher
/// `ceil(bytes_delta * rate / MiB)` arithmetic in `decdn_client::send_voucher`.
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

/// A fresh-lane [`PoolContext`] (all `prior_*` at zero) for `signer` paying
/// `provider` on `pool_id`.
fn fresh_context(pool_id: B256, provider: Address, signer: Arc<PrivateKeySigner>) -> PoolContext {
    PoolContext {
        pool_id,
        provider,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        client_signer: signer,
        voucher_domain: hop_domains().voucher,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    }
}

/// A fresh-lane [`PoolContext`] carrying an ADR 005 client identity binding over
/// the requester's own node id. The pool-model serve gate refuses any request
/// that cannot prove ownership of a lane, so every paid pull attaches a binding
/// signed by the paying key over the connection's node id.
fn bound_context(
    pool_id: B256,
    provider: Address,
    signer: Arc<PrivateKeySigner>,
    own_node_id: B256,
) -> anyhow::Result<PoolContext> {
    let binding = decdn_client::sign_client_binding(&signer, own_node_id, &hop_domains().binding)?;
    Ok(fresh_context(pool_id, provider, signer).with_client_binding(binding))
}

/// Seed the seller store with a fresh lane so the handler admits vouchers on the
/// `(pool_id, signer, provider)` triple: `signer` is the paying client's key,
/// `provider` the serving node's operator address, `cap` the pool deposit. The
/// handler hydrates its in-memory lane map from `load_all` at construction, so a
/// lane recorded before `build_server` is served from the first voucher on.
fn seed_lane(
    store: &MemoryPoolStateStore,
    pool_id: B256,
    signer: Address,
    provider: Address,
) -> anyhow::Result<()> {
    store.record(&LaneState::hydrate(
        pool_id,
        signer,
        provider,
        U256::from(DEPOSIT_MICRO_USDC), // cap
        0,                              // expiry: 0 = untracked, never expires
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    Ok(())
}

/// Assert that `store` holds **exactly** the lane `lane` and that it advanced as
/// expected after one full delivery: full byte count and the
/// `expected_amount(rate)` cumulative amount.
///
/// The exactness (`len() == 1` + matching lane + rate-derived amount) is what
/// makes this a real two-hop assertion rather than two single hops: a bug that
/// charged the wrong lane, applied the wrong rate, or leaked B's buyer-role state
/// into its seller store would change the count or the amount.
fn assert_channel_advanced(
    store: &MemoryPoolStateStore,
    hop: &str,
    lane: LaneKey,
    rate: u64,
) -> anyhow::Result<()> {
    let persisted = store.load_all()?;
    anyhow::ensure!(
        persisted.len() == 1,
        "{hop}: expected exactly one lane, got {}",
        persisted.len()
    );
    let only = store
        .get(lane)?
        .ok_or_else(|| anyhow::anyhow!("{hop}: lane {lane:?} not in store"))?;
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
    store: Arc<dyn PoolStateStore>,
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
        16,
    )
}

/// End-to-end two-hop paid pull-through: A → B (paid pull), then B → client
/// (paid serve). Bytes survive both hops and both off-chain channels advance.
#[tokio::test(flavor = "multi_thread")]
async fn node_to_node_pull_through_two_hops() -> anyhow::Result<()> {
    // Position-varying, not a constant fill: a vectored write that permuted chunks
    // WITHIN one frame would still compare equal against a uniform payload, and this
    // is the only large blob the miss leg carries end to end.
    let payload: Vec<u8> = (0..PAYLOAD_LEN)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let hash = decdn_cache::Hash::new(&payload);

    // --- Node A: upstream holder + server ---------------------------------
    let (cache_a, hash_a, _cache_a_tmp) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());

    // Node B's buyer identity (B pays A on the A↔B lane).
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let a_pool_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    let a_lane = LaneKey {
        pool_id: a_pool_id,
        signer: b_buyer.address(),
        provider: a_eth.address(),
    };
    seed_lane(&store_a, a_pool_id, b_buyer.address(), a_eth.address())?;

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
    let ctx_b_to_a = bound_context(
        a_pool_id,
        a_eth.address(),
        Arc::clone(&b_buyer),
        B256::from(*b_id.as_bytes()),
    )?;
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
    assert_channel_advanced(&store_a, "A<->B", a_lane, RATE_A)?;

    // --- Step 2: build B's cache from the pulled bytes -------------------
    // Stand-in for the runtime wiring this file leaves out: a real node's
    // `NodeOrigin` populates the cache during the miss. The engine's public
    // ingest paths are origin pull-through and the serve-miss pull leg via
    // `CacheEngine::claim_fill` (which wants bao-framed bytes, not plaintext), so
    // `cache_with_blob` builds B's cache by round-tripping the paid-for bytes
    // through a throwaway filesystem origin.
    let (cache_b, hash_b, _cache_b_tmp) = cache_with_blob(&pulled).await?;
    anyhow::ensure!(hash_b == hash, "ingested hash mismatch");

    // --- Node B: server toward the client --------------------------------
    let b_eth = Arc::new(PrivateKeySigner::random());
    let client_signer = Arc::new(PrivateKeySigner::random());
    let b_pool_id = B256::repeat_byte(0xB2);
    let store_b = Arc::new(MemoryPoolStateStore::new());
    let b_lane = LaneKey {
        pool_id: b_pool_id,
        signer: client_signer.address(),
        provider: b_eth.address(),
    };
    seed_lane(
        &store_b,
        b_pool_id,
        client_signer.address(),
        b_eth.address(),
    )?;

    let handler_b = build_server(b_id, &b_eth, cache_b, store_b.clone(), RATE_B)?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    // --- Hop 2: client pulls the blob from B, paying vouchers ------------
    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    let ctx_client_to_b = bound_context(
        b_pool_id,
        b_eth.address(),
        Arc::clone(&client_signer),
        B256::from(*client_id.as_bytes()),
    )?;
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
    assert_channel_advanced(&store_b, "B<->client", b_lane, RATE_B)?;

    // Cross-hop isolation: serving the client (hop 2) must not have touched the
    // A<->B lane. Re-assert A's lane is unchanged at hop-1's accounting.
    assert_channel_advanced(&store_a, "A<->B after hop 2", a_lane, RATE_A)?;

    // `shutdown` reaps both accept loops inside its own deadline, so a panic in
    // either fails the test rather than being silently swallowed — and neither can
    // park the test if an endpoint fails to drain.
    shutdown([task_b, task_a], [&client_ep, &ep_b, &ep_a]).await?;
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
        Address::repeat_byte(0x55), // provider (never reached)
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

    // Join rather than abort, so a panic inside the fake upstream surfaces here
    // instead of being silently dropped. Closing `ep_a` is what ends its accept
    // loop, so the join must come after — and bounded, or a refuser that never
    // returns parks the test.
    shutdown([], [&ep_b, &ep_a]).await?;
    support::reap("refuser", refuser).await?;
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
#[allow(clippy::too_many_lines)]
async fn client_disconnect_mid_stream_leaves_channel_reusable() -> anyhow::Result<()> {
    let payload = vec![0x6Du8; PAYLOAD_LEN];
    let hash = decdn_cache::Hash::new(&payload);
    let (cache, hash_b, _cache_tmp) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_b == hash, "fixture hash mismatch");

    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let b_eth = Arc::new(PrivateKeySigner::random());
    let client_signer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xC1);
    let store = Arc::new(MemoryPoolStateStore::new());
    let lane = LaneKey {
        pool_id,
        signer: client_signer.address(),
        provider: b_eth.address(),
    };
    seed_lane(&store, pool_id, client_signer.address(), b_eth.address())?;

    let handler = build_server(b_id, &b_eth, cache, store.clone(), RATE_B)?;
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let task_b = spawn_server(ep_b.clone(), handler);

    // --- Abort: request, read the response + one chunk, then hang up ---------
    {
        let client_sk = fresh_key();
        let client_node_id = B256::from(*client_sk.public().as_bytes());
        let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
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
            pool_id: pool_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: 0x00c0_ffee,
        };
        // A bound request: the pool-model serve gate refuses any request without a
        // verified client binding (it cannot name a lane), so the abort must prove
        // ownership over the connection's node id before it can reach the stream.
        let binding_hash = binding_signing_hash(
            client_node_id,
            EPHEMERAL_BINDING_NONCE,
            &hop_domains().binding,
        );
        let binding_signature = client_signer
            .sign_hash_sync(&binding_hash)?
            .as_bytes()
            .to_vec();
        let ext = StreamRequestExt {
            binding: Some(ClientBinding {
                ethereum_address: client_signer.address().into(),
                binding_signature,
            }),
            capability: None,
        };
        let req_bytes = encode_stream_request(&req, Some(&ext))
            .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
        write_frame(&mut send, &req_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("write request: {e}"))?;

        match read_client_msg(&mut recv).await? {
            ClientMessage::StreamResponse(r) => {
                anyhow::ensure!(r.body.ok, "expected an ok response, got a refusal");
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
        shutdown([], [&client_ep]).await?;
    }

    // Property 1: no voucher accepted — the lane never advanced.
    // Bytes-delivered must still read zero (its initial state), ruling out any
    // partial accounting committed for the unpaid chunk.
    let after_abort = store
        .get(lane)?
        .ok_or_else(|| anyhow::anyhow!("lane vanished after the abort"))?;
    anyhow::ensure!(
        after_abort.last_bytes_delivered() == U256::ZERO,
        "an aborted pull committed {} bytes of accounting",
        after_abort.last_bytes_delivered()
    );

    // Property 2: the same lane still serves. A fresh-context honest pull
    // completes and advances it (the abort left prior state at zero).
    let honest_sk = fresh_key();
    let honest_id = honest_sk.public();
    let (honest_ep, _) = local_endpoint(honest_sk, vec![]).await?;
    let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    let ctx = bound_context(
        pool_id,
        b_eth.address(),
        Arc::clone(&client_signer),
        B256::from(*honest_id.as_bytes()),
    )?;
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
    assert_channel_advanced(&store, "reused after abort", lane, RATE_B)?;

    shutdown([task_b], [&honest_ep, &ep_b]).await?;
    Ok(())
}

/// A protocol-correct but **dishonest** `cdn/client/v1` upstream: it signs a
/// valid response for the requested hash, streams `served` (whose hash differs),
/// and then reads the closing voucher and ends the stream cleanly if the
/// requester pays — yet the bytes are wrong. This drives the requester all the
/// way to its integrity check. Handles exactly one connection, then returns.
///
/// The requester verifies each bao chunk group as it lands (ADR 038), so it may
/// reject the bytes and close the connection before it pays the closing voucher,
/// or between the voucher and `StreamEnd`. Both orders are legal, and the
/// rejection is the unit under test. A peer close after the bytes are on the wire
/// therefore ends the liar with `Ok`; every other error still fails it.
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
        pool_id: req.pool_id,
        timestamp_us: req.timestamp_us,
    };
    let slash_sig = StreamSlashData::from_response_body(&body)
        .sign(eth.as_ref(), slash)
        .map_err(|e| anyhow::anyhow!("slash sign: {e}"))?
        .as_bytes()
        .to_vec();
    let resp = StreamResponse { body, slash_sig };
    let payload = decdn_protocol::encode_stream_response(
        &resp,
        Some(&decdn_protocol::StreamResponseExt { error: None }),
    )?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;

    // The wrong bytes, streamed in 1 KiB frames — a sender's own choice, since the
    // requester accepts any non-empty frame. `served` stays under one payment
    // chunk, so exactly one closing voucher flows.
    for chunk in served.chunks(1024) {
        write_client_msg(
            &mut send,
            &ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?),
        )
        .await?;
    }

    // Read the closing voucher the requester pays for the bytes it received.
    // Acceptance is implicit — continued delivery is the ack (ADR 005), so the
    // upstream sends no explicit reply and moves straight to `StreamEnd`, letting
    // the requester proceed to the integrity check (the unit under test) rather
    // than bailing early on a rejected voucher.
    let msg = match read_client_msg(&mut recv).await {
        Err(_) if peer_closed(&conn) => return Ok(()),
        msg => msg?,
    };
    let ClientMessage::Voucher(_) = msg else {
        anyhow::bail!("lying upstream: expected a Voucher");
    };
    match write_client_msg(&mut send, &ClientMessage::StreamEnd).await {
        Err(_) if peer_closed(&conn) => return Ok(()),
        written => written?,
    }
    let _ = send.finish();
    // Hold the connection open until the requester closes it. Returning here
    // would drop `conn` and abort the still-in-flight `StreamEnd` before it
    // lands.
    conn.closed().await;
    Ok(())
}

/// Whether the peer closed `conn` itself. A local close, a timeout, or a reset
/// is not a peer close.
fn peer_closed(conn: &Connection) -> bool {
    matches!(
        conn.close_reason(),
        Some(ConnectionError::ApplicationClosed(_))
    )
}

/// Sad path: the upstream returns bytes that don't match the requested hash
/// (#746).
///
/// Content is BLAKE3-addressed and the requester verifies every bao chunk group
/// against the content root (ADR 038) — but that defense had no node-to-node
/// test. Here a malicious / buggy upstream plays the protocol perfectly (valid
/// signed response, and a clean `StreamEnd` once it is paid) while serving
/// content that hashes to the wrong value. The downstream requester MUST reject
/// the delivery and return an `Err`, never surfacing the corrupt bytes to its
/// caller. This guards the content-addressing invariant across a *paid* hop: a
/// peer cannot substitute content for a hash, even when the requester pays for
/// the bytes before it rejects them.
#[tokio::test(flavor = "multi_thread")]
async fn upstream_hash_mismatch_is_rejected() -> anyhow::Result<()> {
    // What the downstream asks for...
    let honest = vec![0x11u8; 4096];
    let requested_hash = decdn_cache::Hash::new(&honest);
    // ...vs. what the lying upstream actually serves (different content, sized
    // under one chunk).
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
        upstream_eth.address(),
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

    shutdown([], [&ep_b]).await?;
    // Surface any panic or protocol error the liar hit, bounded so a hang fails
    // loudly rather than stalls.
    match tokio::time::timeout(Duration::from_secs(5), liar).await {
        Ok(joined) => {
            joined.map_err(|e| anyhow::anyhow!("lying upstream task panicked: {e}"))??;
        }
        Err(_) => anyhow::bail!("lying upstream did not finish in time"),
    }
    shutdown([], [&ep_up]).await?;
    Ok(())
}
