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
    ChannelState, ChannelStateStore, MemoryChannelStateStore, bind_node_id_domain,
    slash_judge_domain, voucher_domain,
};
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::metrics::Metrics;
use decdn_protocol::ALPN_CLIENT;
use iroh::EndpointAddr;

mod support;
use support::{
    HandlerDomains, build_handler_full, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter, spawn_server,
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
/// `rate` per MiB: one full-interval voucher (`rate`) plus a closing voucher for
/// the trailing 0.5 MiB (`ceil(rate/2)`). Matches the requester's per-voucher
/// `ceil(bytes_delta * rate / MiB)` arithmetic in `client_requester::self_pay`.
fn expected_amount(rate: u64) -> U256 {
    U256::from(rate + rate.div_ceil(2))
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
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(PAYLOAD_LEN),
        "{hop}: bytes_delivered {}",
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
    let _ = task_b.await;
    let _ = task_a.await;
    Ok(())
}
