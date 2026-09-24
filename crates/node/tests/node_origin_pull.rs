//! Runtime-orchestrated node-to-node cache-miss pull through `NodeOrigin` (#831).
//!
//! `node_to_node_pull_through.rs` composes the protocol halves by hand because
//! the runtime orchestration did not exist. This binary drives the real
//! orchestration: a [`NodeOrigin`], provisioned with discovery (origin
//! directory), a `NodeId → address` resolver, a buyer-channel opener, and the
//! reputation handles, on a single `Origin::fetch` call:
//!
//! 1. discovers upstream A (DHT lookup is empty → origin-directory fallback),
//! 2. probes A over `cdn/probe/v1` for rate/RTT and ranks it,
//! 3. opens a buyer channel (stubbed — no chain) and pulls over `cdn/client/v1`,
//! 4. returns the hash-verified bytes AND records the delivery outcome into the
//!    local score + the observation buffer (the outbound-report feed, ADR 008).
//!
//! A serves both ALPNs from one endpoint (one NodeId): the real
//! [`ClientHandler`] for the paid pull and a hand-rolled signed probe responder.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use decdn_cache::origin::{FilesystemOrigin, Origin, OriginFetch};
use decdn_cache::{
    CacheEngine, CacheMetrics, CircuitBreakerPolicy, Hash, PinnedHashes, RetryPolicy,
};
use decdn_client::PoolContext;
use decdn_client::probe::probe_once;
use decdn_incentive::{
    EPHEMERAL_BINDING_NONCE, LaneKey, LaneState, MemoryPoolStateStore, PoolStateStore,
    ProbeSlashData, StreamSlashData, Voucher, bind_node_id_domain, binding_signing_hash,
    min_payment, signed_to_wire_voucher, slash_judge_domain, voucher_domain,
};
use decdn_node::buyer_channel::{PoolOpenPending, PoolOpener, TopUpLanded};
use decdn_node::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use decdn_node::dht::{
    ConfigStakerSet, NegativeProbeCache, NodeAddressResolver, OriginDirectory, PositiveProbeCache,
    ProbedProvider, StakerSet, StaticNodeAddressDirectory, StaticOriginDirectory,
};
use decdn_node::metrics::Metrics;
use decdn_node::node_origin::{NodeOrigin, NodeOriginConfig, NodeOriginDeps};
use decdn_node::selection::{
    CHANNEL_OPEN_CALLER_BUDGET, MAX_PROVIDER_ATTEMPTS, outer_pull_deadline,
};
use decdn_protocol::client::{
    ChunkData, ClientBinding, ClientMessage, StreamError, StreamRequest, StreamRequestExt,
    StreamResponse, StreamResponseBody, StreamResponseExt, VoucherRejectReason,
    encode_stream_response,
};
use decdn_protocol::message::{
    ProbeResponse, ProbeResponseBody, ProbeResponseExt, encode_probe_response,
};
use decdn_protocol::{
    ALPN_CLIENT, ALPN_PROBE, CHUNK_BYTES, ContentHash, Coverage, MB_BYTES, ProbeMessage,
    decode_message, encode_message, encode_stream_request, read_frame, write_frame,
};
use decdn_reputation::{LocalReputation, LocalReputationConfig};
use iroh::EndpointAddr;
use iroh::endpoint::Connection;

mod support;
use support::{
    HandlerDomains, build_handler_full, build_handler_full_configured, cache_with_blob,
    empty_cache, fresh_key, local_endpoint, permissive_limiter, shutdown, spawn_server,
};

/// Frame size these hostile-server fixtures cut their wire bytes at.
///
/// A sender's own choice, not a protocol value: `cdn/client/v1` bounds a
/// `ChunkData` payload only as non-empty, so a fixture picks whatever size makes
/// its case legible. 1 KiB keeps each fixture's frame count small enough to reason
/// about while still crossing several frame boundaries.
const WIRE_FRAME: usize = 1024;

const CHAIN_ID: u64 = 421_614;
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// 1.5 MiB → crosses one 1 MiB chunk plus a closing voucher.
const PAYLOAD_LEN: usize = 1_572_864;
const RATE: u64 = 10;
/// A strictly-cheaper quote than [`RATE`] so the stalling provider ranks #1 in
/// the selection score (#859 fallthrough test).
const STALL_RATE: u64 = RATE / 2;

const fn slash_verifying() -> Address {
    Address::repeat_byte(0x11)
}
fn slash_domain() -> Eip712Domain {
    slash_judge_domain(CHAIN_ID, slash_verifying())
}
fn voucher_dom() -> Eip712Domain {
    voucher_domain(CHAIN_ID, Address::repeat_byte(0x34))
}
fn binding_dom() -> Eip712Domain {
    bind_node_id_domain(CHAIN_ID, Address::repeat_byte(0x99))
}

/// A recorded `record_progress` call: `(provider, bytes_delivered, amount)`.
type ProgressEntry = (Address, U256, U256);

/// A stub [`PoolView`] returning a fixed `getPool` status per pool id — the
/// seller's serve gates read the pool OWNER (ADR 011 funder subject) and the
/// pool REMAINING (floor-`M` solvency) from it. An unmapped pool yields `None`,
/// so the gates fail open exactly as they do without a chain view.
#[derive(Debug)]
struct StubPoolView {
    status: HashMap<B256, decdn_node::pool_view::PoolStatus>,
}

#[async_trait]
impl decdn_node::pool_view::PoolView for StubPoolView {
    async fn status(&self, pool_id: B256) -> Option<decdn_node::pool_view::PoolStatus> {
        self.status.get(&pool_id).copied()
    }
}

/// A buyer-channel opener that stands in for the chain-backed
/// `BuyerChannelService`, so the test exercises the pull without a chain.
///
/// It also models the #852 persistence loop: [`PoolOpener::record_progress`]
/// appends to `recorded`, and `open_or_reuse_pool` seeds the returned
/// context's `prior_*` from the latest recorded entry for that provider — exactly
/// what the real store-backed service does on reuse. A second pull therefore
/// resumes from the first pull's watermark instead of re-signing a stale voucher.
#[derive(Debug)]
struct StubOpener {
    pool_id: B256,
    deposit: U256,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    /// `record_progress` calls in order — the test's view of what was persisted.
    recorded: Arc<Mutex<Vec<ProgressEntry>>>,
    /// `retire_channel` calls in order — the test's view of which channels were
    /// rotated out after an upstream said they could never pay again (#1145 review).
    ///
    /// Retirement is modelled, not just logged: once a provider's channel is retired,
    /// `open_or_reuse_pool` stops resuming from its persisted watermark and hands
    /// back a fresh (zeroed) context, which is what the store-backed service does once
    /// the row is gone. A test can therefore tell a channel that was *recorded* as
    /// retired from one that actually stopped being reused.
    retired: Arc<Mutex<Vec<(Address, B256)>>>,
}

#[async_trait]
impl PoolOpener for StubOpener {
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        _budget: Duration,
    ) -> Result<PoolContext> {
        // A retired channel is GONE: the store row was dropped, so there is nothing to
        // resume from and the next open starts clean. Modelling this is what lets a test
        // distinguish "we retired the channel" from "we retired it and then resumed the
        // dead watermark anyway", which would wedge the fresh lane exactly as the
        // retired one is (#1145 review).
        let was_retired = self
            .retired
            .lock()
            .map_err(|_| anyhow::anyhow!("retired lock poisoned"))?
            .iter()
            .any(|(provider, _)| *provider == provider_addr);

        let recorded = self
            .recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?;
        // Resume from the latest persisted watermark for this provider (fresh
        // zeros if none) — the reuse path resumes each lane at its own frontier.
        let (prior_bytes_delivered, prior_amount) = recorded
            .iter()
            .rev()
            .find(|(provider, ..)| *provider == provider_addr)
            .filter(|_| !was_retired)
            .map_or((U256::ZERO, U256::ZERO), |(_, b, a)| (*b, *a));
        Ok(PoolContext {
            pool_id: self.pool_id,
            provider: provider_addr,
            deposit: self.deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_bytes_delivered,
            prior_amount,
            client_binding: None,
            capability: None,
        })
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        pool_id: B256,
        write: decdn_client::buyer_pool::ProgressWrite,
    ) -> Result<()> {
        let totals = write.totals();
        let (bytes_delivered, amount) = (totals.last_bytes, totals.last_amount);
        // The orchestrator must persist progress against the pool it pulled
        // on — i.e. the id from the `PoolContext` it just opened/reused.
        // Asserts the `ctx.pool_id` plumbing at the pull call site (#838).
        anyhow::ensure!(
            pool_id == self.pool_id,
            "record_progress pool_id {pool_id} != opened pool {}",
            self.pool_id
        );
        self.recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?
            .push((provider_addr, bytes_delivered, amount));
        Ok(())
    }
}

/// An opener that hands back a fresh-channel context but whose `record_progress`
/// always fails — models a store-write failure on the persist path so a test can
/// assert the pull still delivers the paid-for bytes (#852: a persist failure
/// must not fail the pull, only surface via the metric + warn).
#[derive(Debug)]
struct FailingRecordOpener {
    pool_id: B256,
    deposit: U256,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
}

/// A [`PoolOpener`] whose open for `wedged` never completes within the caller's
/// budget — the on-chain hazard #1143 exists for (an unresponsive RPC, a
/// `ChannelOpened` tx that never mines). Every other provider opens instantly.
///
/// It ASSUMES the budget contract rather than testing it: it sleeps for `budget`
/// and hands back the typed [`PoolOpenPending`], which is what the real service
/// does — but because this body *re-implements* that behaviour, nothing here would
/// notice if `BuyerChannelService::open_or_reuse_pool` stopped doing it. Scope
/// this fixture to what it genuinely covers: `node_origin`'s candidate loop, i.e.
/// that a pending open is metered, scores no reputation, and falls through to the
/// next candidate. Returning the real sentinel (rather than a bare string) is what
/// makes it reach `record_pool_open_failure`'s pending arm and its counter
/// instead of the generic channel-open-failure arm.
///
/// The singleflight ITSELF — the caller's bound, and the open slot surviving a
/// caller that walks away — is guarded where the mechanism lives:
/// `a_caller_that_times_out_leaves_the_open_slot_held` and
/// `a_second_caller_joins_the_in_flight_open_rather_than_opening_again` in
/// `crates/node/src/buyer_channel.rs`, plus section A0 of the anvil e2e. Neither
/// `StubOpener` nor `FailingRecordOpener` models any of this.
#[derive(Debug)]
struct WedgedOpener {
    /// Every provider whose channel open wedges. A set, not a single address, so a
    /// test can wedge enough candidates to prove the loop still reaches the LAST
    /// one `MAX_PROVIDER_ATTEMPTS` allows.
    wedged: HashSet<Address>,
    pool_id: B256,
    deposit: U256,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    /// Providers whose open was attempted, in order — so a test can prove the loop
    /// actually reached the fallback rather than succeeding for some other reason.
    attempted: Arc<Mutex<Vec<Address>>>,
}

#[async_trait]
impl PoolOpener for WedgedOpener {
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        budget: Duration,
    ) -> Result<PoolContext> {
        if let Ok(mut attempted) = self.attempted.lock() {
            attempted.push(provider_addr);
        }
        if self.wedged.contains(&provider_addr) {
            // Consume exactly the budget, then return the TYPED sentinel — a stand-in
            // for what the real service does once its detached open task has not
            // resolved in time. This is a MODEL of that contract, not a check on it;
            // see the doc above for where the contract itself is guarded.
            //
            // A bare string error would not `downcast_ref::<PoolOpenPending>()`, so
            // `record_pool_open_failure` takes its generic-failure arm instead of the
            // pending one — the test still passes (neither arm scores reputation)
            // while never exercising the path it claims to.
            tokio::time::sleep(budget).await;
            return Err(anyhow::Error::new(PoolOpenPending { waited: budget }));
        }
        Ok(PoolContext {
            pool_id: self.pool_id,
            provider: provider_addr,
            deposit: self.deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        })
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
        _pool_id: B256,
        _write: decdn_client::buyer_pool::ProgressWrite,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl PoolOpener for FailingRecordOpener {
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        _budget: Duration,
    ) -> Result<PoolContext> {
        Ok(PoolContext {
            pool_id: self.pool_id,
            provider: provider_addr,
            deposit: self.deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        })
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
        _pool_id: B256,
        _write: decdn_client::buyer_pool::ProgressWrite,
    ) -> Result<()> {
        anyhow::bail!("simulated buyer-pool store write failure")
    }
}

/// Answer one `cdn/probe/v1` request with a signed `has_blob: true` response.
async fn answer_probe(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    rate: u64,
    total_bytes: u64,
) -> Result<()> {
    // Whole-blob holder: advertises coverage over every discovery block.
    let coverage = Coverage::full(decdn_protocol::num_blocks(total_bytes));
    answer_probe_with_coverage(conn, eth, slash, rate, total_bytes, coverage).await
}

/// [`answer_probe`] with an explicit [`Coverage`], so a PARTIAL holder can
/// advertise only the discovery blocks it serves (#1506 ranged assembly). The
/// two-holder loopback drives one holder covering block 0 and another block 1.
async fn answer_probe_with_coverage(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    rate: u64,
    total_bytes: u64,
    coverage: Coverage,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read probe request: {e}"))?;
    let (msg, _) = decode_message::<ProbeMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode probe request: {e}"))?;
    let ProbeMessage::Request(req) = msg else {
        anyhow::bail!("expected a ProbeMessage::Request");
    };
    let body = ProbeResponseBody {
        hash: req.hash,
        has_blob: true,
        rate_per_mb: rate,
        timestamp_us: req.timestamp_us,
    };
    let slash_sig = ProbeSlashData {
        hash: B256::from(req.hash),
        has_blob: true,
        rate_per_mb: rate,
        timestamp_us: req.timestamp_us,
    }
    .sign(eth.as_ref(), slash)
    .map_err(|e| anyhow::anyhow!("sign probe slash: {e}"))?
    .as_bytes()
    .to_vec();
    let resp = ProbeResponse { body, slash_sig };
    let payload = encode_probe_response(
        &resp,
        Some(&ProbeResponseExt {
            total_bytes: Some(total_bytes),
            coverage,
        }),
    )
    .map_err(|e| anyhow::anyhow!("encode probe response: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write probe response: {e}"))?;
    let _ = send.finish();
    // Hold the connection until the requester has read the response and closed
    // it, rather than collapsing it out from under the read; spawned
    // per-connection, so this never stalls the accept loop.
    conn.closed().await;
    Ok(())
}

/// Spawn A's accept loop, dispatching each connection by negotiated ALPN: the
/// real `ClientHandler` for paid pulls, the hand-rolled responder for probes.
fn spawn_a_server(
    ep: iroh::Endpoint,
    client_handler: Arc<decdn_node::handlers::client::ClientHandler>,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
) -> tokio::task::JoinHandle<()> {
    use iroh::protocol::ProtocolHandler;
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            if conn.alpn() == ALPN_PROBE {
                let eth = Arc::clone(&a_eth);
                let dom = slash.clone();
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                let handler = Arc::clone(&client_handler);
                tokio::spawn(async move {
                    let _ = decdn_node::handlers::client::ClientProtocol::new(handler)
                        .accept(conn)
                        .await;
                });
            }
        }
    })
}

/// Like [`spawn_a_server`], but increments `probes` before answering every
/// `cdn/probe/v1` request — the instrument for the probe-cache tests (#1165). A
/// probe-cache hit is *defined* as "no probe was sent", so those tests assert on
/// this counter at the wire rather than trusting `decdn_probe_cache_hits_total`
/// alone: a broken implementation could increment that counter and probe anyway.
fn spawn_a_probe_counting_server(
    ep: iroh::Endpoint,
    client_handler: Arc<decdn_node::handlers::client::ClientHandler>,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
    probes: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    use iroh::protocol::ProtocolHandler;
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            if conn.alpn() == ALPN_PROBE {
                let eth = Arc::clone(&a_eth);
                let dom = slash.clone();
                let probes = Arc::clone(&probes);
                tokio::spawn(async move {
                    probes.fetch_add(1, Ordering::SeqCst);
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                let handler = Arc::clone(&client_handler);
                tokio::spawn(async move {
                    let _ = decdn_node::handlers::client::ClientProtocol::new(handler)
                        .accept(conn)
                        .await;
                });
            }
        }
    })
}

/// Build B's `NodeOrigin` with stubbed discovery (`providers` for `hash` via the
/// origin directory), a static `addr_map` resolver, a fixed-channel opener, and
/// real reputation/metrics handles. Tests vary `providers`/`addr_map` to drive
/// the discovery / resolution / probe / pull branches.
///
/// Returns the origin, its `CacheEngine` (so a test can read pulled bytes back
/// from the store after `AlreadyAdmitted`), and the [`StubOpener`]'s `recorded`
/// log so a test can assert what voucher progress was persisted (#852).
#[allow(clippy::too_many_arguments)]
async fn provisioned_origin(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    pool_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
) -> (
    NodeOrigin,
    CacheEngine,
    Arc<Mutex<Vec<ProgressEntry>>>,
    tempfile::TempDir,
) {
    provisioned_origin_with_deadlines(
        ep_b,
        b_dht,
        hash,
        pool_id,
        buyer_signer,
        local_rep,
        metrics,
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES,
        0,
    )
    .await
}

/// The `(pull_timeout, stall_timeout)` every test that does not care about the deadline gate
/// runs with.
///
/// These bounds exist to stop a wedged fixture from hanging, not to be measured against: no
/// test that uses them asserts on either one firing. So they are sized to be UNREACHABLE by a
/// pull that is making honest progress, however slowly, and 20s was not — a fixture that costs
/// 1.3s uncontended has been seen taking 21.4s and ending on the stall bound, both on the
/// coverage-instrumented four-core CI runner and in an ordinary local whole-package run. The
/// real backstop for a genuine wedge is the `.config/nextest.toml` per-test cap (180s for this
/// package), which these stay clear of at a minute apiece.
///
/// Tests that ASSERT on the deadline gate pass their own budgets and must not use these — two
/// of them feed the same values to `selection::outer_pull_deadline` and check what it derives,
/// so widening these would put the config and that derivation out of step.
const DEFAULT_TEST_PULL_DEADLINES: (Duration, Duration) =
    (Duration::from_mins(1), Duration::from_mins(1));

/// Like [`provisioned_origin`], but the caller picks the node-origin
/// deadline budgets.
///
/// The interesting value is a ZERO one. `NodeOriginConfig::deadlines()` refuses it and marks
/// the error `LocalPullFault`, because a zero window makes the throughput floor unsatisfiable
/// and would abandon every upstream on the first poll of every read (#1797).
///
/// It is the one local fault a test can induce at OPEN time, which is what makes it the only
/// handle a WIRE-level test has on the #1560 path: the fault has to land before the
/// `StreamResponse` is signed, or the serve handler has no refusal code left to pick. (A
/// `BadSignature` voucher rejection also yields `OurLocalFault` and needs no forged signer,
/// but it arrives mid-stream — see
/// `a_local_fault_on_one_candidate_does_not_sink_a_walk_that_still_delivers`.) The PAID pull
/// never reaches the wire under a zero budget; a live probe still may, if the caller wired
/// one.
#[allow(clippy::too_many_arguments)]
async fn provisioned_origin_with_deadlines(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    pool_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    (pull_timeout, stall_timeout): (Duration, Duration),
    max_blob_size_bytes: u64,
) -> (
    NodeOrigin,
    CacheEngine,
    Arc<Mutex<Vec<ProgressEntry>>>,
    tempfile::TempDir,
) {
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(buyer_signer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, engine_tmp) = build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        providers,
        addr_map,
        pull_timeout,
        stall_timeout,
        max_blob_size_bytes,
    )
    .await;
    (origin, engine, recorded, engine_tmp)
}

/// Like [`provisioned_origin`], but the buyer enforces a `max_blob_size_bytes`
/// ceiling — drives the production `pull_from_candidate` path against the
/// buyer-side gate (#840).
#[allow(clippy::too_many_arguments)]
async fn provisioned_origin_with_ceiling(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    pool_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    max_blob_size_bytes: u64,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(buyer_signer),
        voucher_domain: voucher_dom(),
        recorded,
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        max_blob_size_bytes,
    )
    .await
}

/// Provision a `NodeOrigin` with stubbed discovery/resolver/reputation around a
/// caller-supplied buyer `PoolOpener`, so a test can inject any opener
/// (recording, failing, …) without re-wiring the deps.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES.0,
        DEFAULT_TEST_PULL_DEADLINES.1,
        0,
    )
    .await
}

/// [`build_origin`] with an explicit per-candidate `pull_timeout`, so a test can
/// drive a *short* deadline and exercise the stall-then-fallthrough path (#859)
/// without a 20-second wait.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin_with_timeout(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
    max_blob_size_bytes: u64,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    build_origin_with_negative_cache(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        providers,
        addr_map,
        pull_timeout,
        stall_timeout,
        max_blob_size_bytes,
        NegativeProbeCache::new(),
    )
    .await
}

/// [`build_origin_with_timeout`] with the negative cache injected, so a test can pick its
/// CACHE-WIDE TTL.
///
/// That knob is what makes the refusal-TTL split observable at all. The cache anchors expiry
/// on `Instant`, so `tokio::time` cannot fast-forward it, and the production values (5 min
/// cache-wide vs 30 s `REFUSAL_SUPPRESSION_TTL`) are far too long to wait out. Shrinking the
/// cache-wide TTL below `REFUSAL_SUPPRESSION_TTL` INVERTS their order, which is exactly what
/// a test wants: the two arms then have visibly different lifetimes in opposite directions,
/// and no single implementation can satisfy both assertions by accident.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin_with_negative_cache(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
    max_blob_size_bytes: u64,
    negative_cache: NegativeProbeCache,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    build_origin_with_probe_caches(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        providers,
        addr_map,
        pull_timeout,
        stall_timeout,
        max_blob_size_bytes,
        negative_cache,
        PositiveProbeCache::new(),
        // Buffered/generic path: the directory is keyed under `NO_NAMESPACE`,
        // matching the hash-only `Origin::fetch` lookup (node_origin.rs discover).
        U256::ZERO,
        // Reactive mid-pull top-up off; only the #1530 tests turn it on.
        U256::ZERO,
    )
    .await
}

/// [`build_origin_with_timeout`] with BOTH probe caches injected, so a test can pick each
/// TTL independently.
///
/// The positive cache anchors expiry on `Instant` like its negative twin, so `tokio::time`
/// cannot fast-forward it and 15s per assertion is not a test suite. Injecting the two
/// separately is also what makes their INTERACTION observable: a positive entry that
/// outlives a negative one is how a peer becomes selectable again without a re-probe.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin_with_probe_caches(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    _hash: Hash,
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
    max_blob_size_bytes: u64,
    negative_cache: NegativeProbeCache,
    probe_cache: PositiveProbeCache,
    // Namespace the directory keys the provider set under. Buffered/generic callers
    // pass `U256::ZERO` (the generic `Origin::fetch` path looks the directory up
    // under `NO_NAMESPACE`); a namespace-aware progressive test passes a non-zero id
    // so a lookup under `NO_NAMESPACE` resolves nothing (proving the request's
    // namespace actually threads through `discover`).
    directory_namespace: U256,
    // The reactive mid-pull top-up target (#1530). `U256::ZERO` — what every caller
    // but the reactive-top-up tests passes — DISABLES the leg, so a fixture whose
    // channel runs dry still fails the way its test asserts.
    working_deposit: U256,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    // Providers are active stakers, matching production (a probe-cache HIT
    // re-checks `is_active`, so an empty set would make every cached provider
    // un-servable on a hit). `find_providers` still returns empty for them — no
    // routing entries — so fetch #1 resolves via the directory under
    // `directory_namespace`. Built before `providers` is moved into `dir`.
    let stakers = ConfigStakerSet::new(providers.iter().copied().collect());
    let mut dir = HashMap::new();
    dir.insert(directory_namespace, providers);

    let (engine, engine_tmp) = throwaway_engine()
        .await
        .expect("throwaway engine for the node-origin fixture");
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(b_dht))),
        staker_set: Arc::new(stakers) as Arc<dyn StakerSet>,
        origin_directory: Arc::new(StaticOriginDirectory::new(dir)) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: b_dht,
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(local_rep),
        negative_cache,
        probe_cache,
        metrics: Arc::clone(metrics),
        registry_regions: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        config: NodeOriginConfig {
            probe_fanout: 5,
            // Taken verbatim from the caller. Two fixtures feed the same budgets to
            // `selection::outer_pull_deadline` and assert on what it derives, so a
            // margin applied behind the caller's back would put the config and that
            // derivation out of step. Callers that want a generous bound ask for it
            // by name — see [`DEFAULT_TEST_PULL_DEADLINES`].
            pull_timeout,
            stall_window: stall_timeout,
            min_throughput_bps: 0,
            max_blob_size_bytes,
            max_rate_per_mb: 0,
            working_deposit,
            seller_reserve: U256::ZERO,
            // A day's margin; the fixtures use never-expiring channels, so the
            // near-expiry guard (#1603) is inert unless a test sets an expiry.
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
            serve_economics: std::sync::Arc::new(decdn_node::serve_economics::OffPolicy),
            operator_shares: decdn_node::fee_shares::OperatorShares::new(6000),
            frequency_estimator: None,
            sell_rate_base: 0,
            // A budget far larger than any test's buy cost, so these fixtures warm
            // freely and the ADR 041 gate never changes their behaviour.
            warming: std::sync::Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
                1_000_000_000,
                0,
            )),
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        engine: engine.clone(),
    });
    (origin, engine, engine_tmp)
}

/// [`build_origin_with_timeout`] with a DETERMINISTIC ranked order, by pre-seeding
/// the positive probe cache so the pull takes the cached-candidates path instead of
/// live probing.
///
/// The live probe measures a real localhost RTT, and RTT is a MULTIPLICATIVE term in
/// the selection score (`rate_per_mb × rtt_ms × 1/rep²`, `selection::compute_score`):
/// on a loaded CI runner one candidate's probe RTT can inflate past another's and flip
/// their relative order. A test that needs a SPECIFIC order — cheap stallers strictly
/// ahead of a pricier honest fallback — is otherwise racy (a staller ranked BEHIND the
/// honest node is never tried, so its per-candidate timeout never fires). Seeding every
/// provider at the SAME `rtt_ms` collapses the RTT factor to a shared constant, so with
/// the OTHER two score terms also uniform here — every candidate is at the cold-start
/// neutral reputation (no delivery has scored anyone yet), and `1/rep²` is therefore the
/// same for all — the score reduces to `rate_per_mb` alone and the order is
/// load-independent. Equal-rate candidates (the stallers) still tie, and the ranker's geo
/// / RNG tie-break decides their MUTUAL order, but a distinctly pricier fallback lands in
/// its own higher-score group and stays strictly last regardless. The real channel-open +
/// stream fallthrough these tests exercise is untouched — only probe+rank is bypassed.
///
/// `ranked` lists `(provider, quoted rate)`. On a cache hit `cached_candidates` does NOT
/// re-probe: it rebuilds reputation and region fresh but reuses the cached
/// `(rate_per_mb, rtt_ms)`, then re-runs `rank_candidates` — so the seeded rate + fixed
/// RTT are what feed the score. The slice order is only the provider set (also used to
/// build the active staker set the cached path re-checks); the rank is recomputed.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin_seeded_ranking(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    ranked: &[(DhtNodeId, u64)],
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    let probe_cache = PositiveProbeCache::new();
    probe_cache.insert(
        ContentHash::from_bytes(*hash.as_bytes()),
        ranked
            .iter()
            .map(|&(node_id, rate_per_mb)| ProbedProvider {
                node_id,
                rate_per_mb,
                // Identical across candidates: the ranker's RTT term must not vary,
                // or a loaded runner's live-probe jitter reappears through the cache.
                rtt_ms: 1,
                // Whole-blob coverage — these fixtures serve the whole blob, so a
                // cache-hit candidate must range-plan as covering everything.
                coverage: decdn_protocol::Coverage::full(1),
                // No size hint: the pull leg's first open reads the size.
                total_bytes_hint: None,
            })
            .collect(),
    );
    let providers = ranked.iter().map(|&(id, _)| id).collect();
    build_origin_with_probe_caches(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        providers,
        addr_map,
        pull_timeout,
        stall_timeout,
        0,
        NegativeProbeCache::new(),
        probe_cache,
        U256::ZERO,
        // Reactive mid-pull top-up off; only the #1530 tests turn it on.
        U256::ZERO,
    )
    .await
}

/// Build a `NodeOrigin` whose directory maps SEVERAL hashes to the same provider set, so a
/// test can pull two different blobs from one provider through one shared `deps` — and thus
/// one shared `wedged_providers` map (#1145 review). `max_blob_size_bytes` is 0 (no ceiling).
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin_multi_hash(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hashes: &[Hash],
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: &[DhtNodeId],
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    let mut dir = HashMap::new();
    for _h in hashes {
        dir.insert(U256::ZERO, providers.to_vec());
    }
    let (engine, engine_tmp) = throwaway_engine()
        .await
        .expect("throwaway engine for the node-origin fixture");
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(b_dht))),
        // Providers are active stakers, matching production (a probe-cache HIT
        // re-checks `is_active`, so an empty set would make every cached provider
        // un-servable on a hit). `find_providers` still returns empty for them —
        // no routing entries — so fetch #1 resolves via the directory as before.
        staker_set: Arc::new(ConfigStakerSet::new(providers.iter().copied().collect()))
            as Arc<dyn StakerSet>,
        origin_directory: Arc::new(StaticOriginDirectory::new(dir)) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: b_dht,
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(local_rep),
        negative_cache: NegativeProbeCache::new(),
        probe_cache: PositiveProbeCache::new(),
        metrics: Arc::clone(metrics),
        registry_regions: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout,
            stall_window: stall_timeout,
            min_throughput_bps: 0,
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            // Reactive mid-pull top-up OFF (#1530): this fixture asserts what a pull
            // does when its channel runs dry, which a self-funding one would hide.
            working_deposit: U256::ZERO,
            seller_reserve: U256::ZERO,
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
            serve_economics: std::sync::Arc::new(decdn_node::serve_economics::OffPolicy),
            operator_shares: decdn_node::fee_shares::OperatorShares::new(6000),
            frequency_estimator: None,
            sell_rate_base: 0,
            // A budget far larger than any test's buy cost, so these fixtures warm
            // freely and the ADR 041 gate never changes their behaviour.
            warming: std::sync::Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
                1_000_000_000,
                0,
            )),
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        engine: engine.clone(),
    });
    (origin, engine, engine_tmp)
}

/// Convenience: a single-provider directory entry + its resolver binding.
fn one_provider(
    a_dht: DhtNodeId,
    a_eth_addr: Address,
) -> (Vec<DhtNodeId>, HashMap<DhtNodeId, Address>) {
    let mut addr_map = HashMap::new();
    addr_map.insert(a_dht, a_eth_addr);
    (vec![a_dht], addr_map)
}

/// Snapshot the `StubOpener`'s persisted-progress log (the #852 watermark the
/// pull path recorded via `record_progress`).
fn progress_log(recorded: &Arc<Mutex<Vec<ProgressEntry>>>) -> Result<Vec<ProgressEntry>> {
    let log = recorded
        .lock()
        .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?;
    Ok(log.clone())
}

/// Assert a `decdn_<name> <value>` counter line is present in the metrics text.
fn assert_counter(metrics: &Arc<Metrics>, name: &str, value: u64) -> Result<()> {
    let text = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
    let want = format!("decdn_{name} {value}");
    anyhow::ensure!(
        text.lines().any(|l| l == want),
        "expected metric line `{want}`; got:\n{text}"
    );
    Ok(())
}

/// A relay node that pulled a `blob_len`-byte blob from upstream and served it
/// downstream records one completed outbound stream and counts at least the
/// blob in both byte directions. The pull thread marks its end just after the
/// last byte lands, so this waits briefly for it.
async fn assert_relay_counted(metrics: &Arc<Metrics>, blob_len: u64) -> Result<()> {
    let outbound_completed = "streams_completed_total{direction=\"outbound\"}";
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while counter_value(metrics, outbound_completed)? == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_counter(metrics, outbound_completed, 1)?;
    assert_counter(metrics, "streams_failed_total{direction=\"outbound\"}", 0)?;
    anyhow::ensure!(
        counter_value(metrics, "bytes_received_total")? >= blob_len,
        "the relay must count the blob it pulled"
    );
    anyhow::ensure!(
        counter_value(metrics, "bytes_served_total")? >= blob_len,
        "the relay must count the blob it served"
    );
    Ok(())
}

/// Read a `decdn_<name>` counter's value from the metrics scrape, or `0` if the
/// line is absent.
fn counter_value(metrics: &Arc<Metrics>, name: &str) -> Result<u64> {
    let text = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
    let prefix = format!("decdn_{name} ");
    let val = text
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    Ok(val)
}

/// A fresh, empty destination cache for a `NodeOrigin`'s node-to-node pull to
/// stream into (#1682): `NodeOriginDeps.engine` is where `pull_from_candidate`
/// admits each gap via `admit_bao_stream`, so every fixture that provisions an
/// origin needs one, whether or not the fixture reads its contents back.
///
/// Returns the `TempDir` guard alongside the engine: the builders below thread it
/// through to their own caller, which binds it for the life of the test so its drop
/// reclaims the backing directory.
async fn throwaway_engine() -> Result<(CacheEngine, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let engine = CacheEngine::open(dir.path(), vec![], 16).await?;
    Ok((engine, dir))
}

/// Build a cache pre-seeded with every payload in `payloads`: a one-shard
/// filesystem origin holds them, the cache pulls each into its local store, then
/// the origin is dropped. A multi-blob sibling of `support::cache_with_blob`,
/// used where a node must hold more than one blob.
async fn cache_with_blobs(payloads: &[&[u8]]) -> Result<(CacheEngine, tempfile::TempDir)> {
    let origin_dir = tempfile::tempdir()?;
    for payload in payloads {
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
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;
    for payload in payloads {
        let _ = cache.get(Hash::new(payload)).await?; // populate the local store
    }
    drop(origin_dir);
    Ok((cache, cache_dir))
}

/// Build a cache whose local store is EMPTY but whose filesystem origin holds
/// `payload`, wired with an `Arc<CacheMetrics>` so a caller can assert
/// `origin_fetches` — the counter that pins whether the node actually read its
/// origin (`1`) or short-circuited (`0`). Both temp dirs are returned so the
/// caller keeps the origin alive across a pull. Used by the #1117 chained-pull
/// test: the upstream must REACTIVELY pull its own origin (authorized by the
/// requester's ADR 005 binding) to serve, since nothing is pre-warmed.
async fn cache_with_fs_origin_only(
    payload: &[u8],
) -> Result<(
    CacheEngine,
    Hash,
    Arc<CacheMetrics>,
    tempfile::TempDir,
    tempfile::TempDir,
)> {
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
    let metrics = Arc::new(CacheMetrics::default());
    let cache = CacheEngine::open_full(
        cache_dir.path(),
        vec![origin as Arc<dyn Origin>],
        16,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&metrics)),
        Duration::ZERO,
    )
    .await?;
    anyhow::ensure!(!cache.has(hash).await?, "upstream store must start empty");
    Ok((cache, hash, metrics, origin_dir, cache_dir))
}

/// #1117: node→node CHAINED reactive pull-through. Node B pulls a blob from
/// upstream A via its `NodeOrigin`; A does NOT hold the blob in its store — only
/// in its own filesystem origin — so A can serve only by REACTIVELY pulling its
/// origin, which A's `pull_authorized` gate allows solely because B now attaches
/// an ADR 005 client identity binding (over B's OWN node id, signed with the
/// channel's buyer key). B receiving the bytes, and A's origin fetching exactly
/// once, proves the binding propagated across the hop and authorized the chained
/// pull. Pre-#1117 (B sent no binding) A refused with `NotFound`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_pull_chains_reactive_origin_via_client_binding() -> Result<()> {
    let payload = vec![0x9Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A (upstream): blob ONLY in its fs origin; store starts empty. ----
    let (cache_a, hash_a, cache_a_metrics, _origin_tmp_a, _cache_tmp_a) =
        cache_with_fs_origin_only(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xB1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    // A reactively serves its OWN origin on a miss (#1116) — the chained pull B
    // triggers. `pull_authorized` still gates it on B proving channel ownership.
    let handler_a = build_handler_full_configured(
        a_id,
        &a_eth,
        &metrics_a,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
        |deps| deps.local_populate = Some(Duration::from_secs(20)),
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin (now sends a binding).
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, engine, _recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        pool_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    // Discover → probe → open (with binding) → A reactively pulls its own origin
    // → serve. A held nothing in-store, so a successful pull is proof of the
    // chained reactive fill.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin chained fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "node-origin did not admit the blob; the chained reactive pull did not fire"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "chained-pull bytes mismatch"
    );
    anyhow::ensure!(
        cache_a_metrics.origin_fetches.get() == 1,
        "upstream A must reactively pull its origin exactly once, got {}",
        cache_a_metrics.origin_fetches.get()
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// Headline #831 test: a provisioned `NodeOrigin` fills a miss by paid-pulling
/// from an upstream node and records the delivery into the reputation feeds.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_pull_fills_and_records_reputation() -> Result<()> {
    let spans = support::capture_spans();
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client over one endpoint. -----
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    // Prime B's iroh address cache with A's address via one explicit-addr probe,
    // so the orchestration's `EndpointAddr::new(a_id)` (no addr) resolves — the
    // same priming the DHT integration tests rely on for NodeId-only dialing.
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, engine, recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        pool_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    // --- The orchestration: discover → probe → rank → open → pull. ------------
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "node-origin did not admit the blob; expected the pull to deliver it"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "pulled bytes mismatch"
    );

    // A's local score rose above the 0.5 neutral after a clean delivery.
    anyhow::ensure!(
        local_rep.score(a_id) > 0.5,
        "local score should rise after a clean delivery, got {}",
        local_rep.score(a_id)
    );
    // Observability: the success + attempt counters moved.
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;

    // #852: the buyer persisted the channel's voucher watermark after the pull.
    // Under ADR 038 the pull meters WIRE bytes (the bao stream: content +
    // interleaved proof), so the closing watermark carries nonce 2, the
    // bao-encoded size, and the cumulative amount rounded up per the rate.
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(total_bytes, &bao_tree::ChunkRanges::all());
    let expected_amount = U256::from(expected_wire)
        .saturating_mul(U256::from(RATE))
        .div_ceil(U256::from(MB_BYTES));
    anyhow::ensure!(
        progress_log(&recorded)?
            == vec![(a_eth.address(), U256::from(expected_wire), expected_amount)],
        "expected one persisted progress entry with the final voucher totals, got {:?}",
        progress_log(&recorded)?
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;

    // The pull runs on its own thread and runtime; its spans must still nest
    // under the fetch: node_pull ⊃ upstream_stream ⊃ open_progressive_pull.
    let key = hash.to_string();
    let pulls = spans.matching("upstream_stream", "hash", &key);
    anyhow::ensure!(
        pulls.iter().any(|s| s.parent == Some("node_pull")
            && s.fields.get("outcome").map(String::as_str) == Some("filled")),
        "upstream_stream: {pulls:?}"
    );
    let opens = spans.matching("open_progressive_pull", "hash", &key);
    anyhow::ensure!(
        !opens.is_empty()
            && opens.iter().all(|s| s.parent == Some("upstream_stream")
                && s.fields.contains_key("byte_offset")
                && s.fields.contains_key("byte_len")),
        "open_progressive_pull must nest under upstream_stream with its range: {opens:?}"
    );
    Ok(())
}

/// A large node-to-node populate lands the blob complete and durable, driven
/// through the streaming pull (no whole-blob RAM buffer). #1682.
///
/// Twin of [`node_origin_pull_fills_and_records_reputation`], but driven through
/// `CacheEngine::populate` rather than a bare `Origin::fetch` call, and over a
/// blob several chunk groups past the old 4 MiB buffered threshold — the point
/// of the port is only exercised once the pull spans more than one gap-driven
/// range.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn large_blob_populates_via_streaming_pull() -> Result<()> {
    const LARGE_PAYLOAD_LEN: usize = 8 * 1024 * 1024;
    let mut payload = vec![0u8; LARGE_PAYLOAD_LEN];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut payload {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(LARGE_PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client over one endpoint. -----
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA2);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin, whose own cache is
    // the destination the streaming pull admits into. -------------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());

    // Deferred-initialisation pattern (see `node_origin` module docs): the
    // `NodeOrigin` is built empty, placed in node B's own cache's origin chain,
    // then provisioned with that SAME cache as `NodeOriginDeps.engine` — the
    // streaming pull admits straight into it.
    let origin = NodeOrigin::new();
    let cache_dir_b = tempfile::tempdir()?;
    let engine_b = CacheEngine::open(
        cache_dir_b.path(),
        vec![Arc::new(origin.clone()) as Arc<dyn Origin>],
        16,
    )
    .await?;
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let mut dir = HashMap::new();
    dir.insert(U256::ZERO, providers.clone());
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(DhtNodeId::from_bytes(
            *b_id.as_bytes(),
        )))),
        staker_set: Arc::new(ConfigStakerSet::new(providers.into_iter().collect()))
            as Arc<dyn StakerSet>,
        origin_directory: Arc::new(StaticOriginDirectory::new(dir)) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: DhtNodeId::from_bytes(*b_id.as_bytes()),
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(&local_rep),
        negative_cache: NegativeProbeCache::new(),
        probe_cache: PositiveProbeCache::new(),
        metrics: Arc::clone(&b_metrics),
        registry_regions: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        config: NodeOriginConfig {
            probe_fanout: 5,
            // Generous by intent, like [`DEFAULT_TEST_PULL_DEADLINES`]: a loaded
            // runner must not end a pull this fixture is not measuring.
            pull_timeout: DEFAULT_TEST_PULL_DEADLINES.0,
            stall_window: DEFAULT_TEST_PULL_DEADLINES.1,
            min_throughput_bps: 0,
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            working_deposit: U256::ZERO,
            seller_reserve: U256::ZERO,
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
            serve_economics: std::sync::Arc::new(decdn_node::serve_economics::OffPolicy),
            operator_shares: decdn_node::fee_shares::OperatorShares::new(6000),
            frequency_estimator: None,
            sell_rate_base: 0,
            // A budget far larger than any test's buy cost, so these fixtures warm
            // freely and the ADR 041 gate never changes their behaviour.
            warming: std::sync::Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
                1_000_000_000,
                0,
            )),
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        engine: engine_b.clone(),
    });

    // --- Populate node B's cache: discover → probe → rank → open → stream. ----
    engine_b.populate(hash).await?;

    anyhow::ensure!(
        engine_b.has(hash).await?,
        "blob must be present after populate"
    );
    let got = engine_b.get(hash).await?;
    anyhow::ensure!(got.len() as u64 == total_bytes, "full blob length");
    anyhow::ensure!(got.as_ref() == payload.as_slice(), "content matches");

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// A protocol-correct but dishonest upstream over `cdn/client/v1`: signs a valid
/// response for the requested hash, then streams `served` (whose hash differs),
/// acks the closing voucher, and ends cleanly. Drives the requester to its
/// whole-blob integrity check (modelled on `node_to_node_pull_through`'s
/// `lying_upstream`).
async fn serve_wrong_bytes(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    served: &[u8],
    rate: u64,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read request: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode request: {e}"))?
            .0
    };
    let ClientMessage::StreamRequest(req) = req_msg else {
        anyhow::bail!("lying upstream: expected a StreamRequest");
    };
    let body = StreamResponseBody {
        hash: req.hash,
        ok: true,
        rate_per_mb: rate,
        total_bytes: u64::try_from(served.len()).unwrap_or(u64::MAX),
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
    for chunk in served.chunks(WIRE_FRAME) {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    // Read the closing voucher; acceptance is implicit (continued delivery is the
    // ack, ADR 005), so the server just proceeds to the integrity check.
    let voucher_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode voucher: {e}"))?
            .0
    };
    let ClientMessage::Voucher(_) = voucher_msg else {
        anyhow::bail!("expected a closing Voucher, got {voucher_msg:?}");
    };
    write_frame(&mut send, &encode_message(&ClientMessage::StreamEnd)?)
        .await
        .map_err(|e| anyhow::anyhow!("write end: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Spawn A serving probes truthfully (`has_blob`) but lying on the client
/// stream (wrong bytes), for the corruption-classification test.
fn spawn_a_lying_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    served: Vec<u8>,
    advertised_bytes: u64,
    rate: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let served = served.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, advertised_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_wrong_bytes(conn, &eth, &dom, &served, rate).await;
                });
            }
        }
    })
}

/// A protocol-correct upstream that serves the *right* bytes but pauses, on the
/// client stream, between reading B's `StreamRequest` and emitting any bytes: it
/// fires `received` once B's upstream request lands, then blocks on `release`
/// before streaming. Because B becomes the `claim_fill` Owner *before* it dials
/// upstream, the `received` signal proves B's owner pull is in flight and B's
/// cache is still empty — so a test can open a second same-hash request against B
/// while the gate is held and deterministically drive it into the `claim_fill`
/// Attach branch (#895/#305: one upstream pull, no double spend). Modelled on
/// [`serve_wrong_bytes`] but honest + gated.
async fn serve_gated_correct_bytes(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    served: &[u8],
    rate: u64,
    received: &tokio::sync::Notify,
    release: &tokio::sync::Notify,
) -> Result<()> {
    // The DECOUPLED serve-miss (#1621) opens its upstream through a header
    // handshake. When the probe reported the size, that handshake opens the pull
    // leg's first leg (`byte_len > 0`) and the pull leg adopts it (#2063): one
    // bi-stream. Without a size it is a whole-tail open (`byte_offset == 0 &&
    // byte_len == 0`) the buyer ABORTS right after reading `total_bytes` (it pulls
    // no chunk, pays no voucher, records NO watermark), and the real `PeerSource`
    // range pull follows on a SECOND bi-stream of the same connection.
    //
    // So loop over the connection's bi-streams, and answer every request's header
    // at once: the buyer must read `total_bytes` to sign its own response and claim
    // its fill before the test opens the coalescing request. Gate ONLY the chunk
    // data of a real pull, and never signal `received` for a whole-tail handshake,
    // so the test's `received` wait resolves on the real owner pull (the in-flight
    // tee) landing, and the single `release` reaches the pull that actually blocks
    // on it. A handshake records no watermark, so `upstream.len() == 1` (single
    // SPEND) holds either way.
    loop {
        // No further stream on this connection (the buyer finished on the handshake
        // alone, or opened the real pull on a fresh connection handled by another
        // invocation) surfaces as an `accept_bi` error — nothing left to serve here.
        let Ok((mut send, mut recv)) = conn.accept_bi().await else {
            return Ok(());
        };
        let req_msg = {
            let frame = read_frame(&mut recv)
                .await
                .map_err(|e| anyhow::anyhow!("read request: {e}"))?;
            decode_message::<ClientMessage>(&frame)
                .map_err(|e| anyhow::anyhow!("decode request: {e}"))?
                .0
        };
        let ClientMessage::StreamRequest(req) = req_msg else {
            anyhow::bail!("gated upstream: expected a StreamRequest");
        };
        let body = StreamResponseBody {
            hash: req.hash,
            ok: true,
            rate_per_mb: rate,
            total_bytes: u64::try_from(served.len()).unwrap_or(u64::MAX),
            pool_id: req.pool_id,
            timestamp_us: req.timestamp_us,
        };
        let slash_sig = StreamSlashData::from_response_body(&body)
            .sign(eth.as_ref(), slash)
            .map_err(|e| anyhow::anyhow!("slash sign: {e}"))?
            .as_bytes()
            .to_vec();
        let resp = StreamResponse { body, slash_sig };
        let resp_ext = StreamResponseExt { error: None };
        write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
            .await
            .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
        if req.byte_offset == 0 && req.byte_len == 0 {
            // Whole-tail header handshake: loop back to accept the real pull's
            // bi-stream. The buyer aborts after the header, so there is no voucher
            // exchange to await.
            let _ = send.finish();
            continue;
        }
        // The real range pull landed (B's claim_fill Owner pull is in flight, cache still empty);
        // hold its bytes until the test has opened the coalescing second request.
        received.notify_one();
        release.notified().await;
        for chunk in served.chunks(WIRE_FRAME) {
            write_frame(
                &mut send,
                &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
            )
            .await
            .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
        }
        // Ack the closing voucher so the buyer proceeds to the integrity check (a
        // sub-interval blob produces exactly one closing voucher). Fail fast on
        // anything else so a future protocol drift surfaces here, not as an opaque
        // buyer-side stall.
        let voucher_msg = {
            let frame = read_frame(&mut recv)
                .await
                .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
            decode_message::<ClientMessage>(&frame)
                .map_err(|e| anyhow::anyhow!("decode voucher: {e}"))?
                .0
        };
        let ClientMessage::Voucher(_) = voucher_msg else {
            anyhow::bail!("gated upstream: expected a closing Voucher, got {voucher_msg:?}");
        };
        write_frame(&mut send, &encode_message(&ClientMessage::StreamEnd)?)
            .await
            .map_err(|e| anyhow::anyhow!("write end: {e}"))?;
        let _ = send.finish();
        conn.closed().await;
        return Ok(());
    }
}

/// Spawn the gated honest upstream (see [`serve_gated_correct_bytes`]). Answers
/// probes truthfully; gates only the client stream. The returned `received` /
/// `release` notifies coordinate the test's hand-off.
#[allow(clippy::too_many_arguments)]
fn spawn_a_gated_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    served: Vec<u8>,
    advertised_bytes: u64,
    rate: u64,
    received: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let served = served.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, advertised_bytes).await;
                });
            } else {
                let received = Arc::clone(&received);
                let release = Arc::clone(&release);
                tokio::spawn(async move {
                    let _ = serve_gated_correct_bytes(
                        conn, &eth, &dom, &served, rate, &received, &release,
                    )
                    .await;
                });
            }
        }
    })
}

/// Spin up a gated honest upstream A holding `payload`. Same return shape as
/// [`spawn_lying_node_a`] plus the `received` / `release` gate handles. No
/// channel store is needed — the hand-rolled server acks the buyer's voucher
/// directly.
async fn spawn_gated_node_a(
    payload: &[u8],
) -> Result<(
    iroh::PublicKey,
    std::net::SocketAddr,
    Arc<PrivateKeySigner>,
    iroh::Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<tokio::sync::Notify>,
    Arc<tokio::sync::Notify>,
)> {
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let received = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let advertised_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let task_a = spawn_a_gated_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload.to_vec(),
        advertised_bytes,
        RATE,
        Arc::clone(&received),
        Arc::clone(&release),
    );
    Ok((a_id, addr_a, a_eth, ep_a, task_a, received, release))
}

/// Like `support::spawn_server` but spawns a task per accepted connection instead
/// of awaiting `handler.accept` inline, so node B can serve concurrent client
/// connections. The inline server serializes connections (fine for one-leaf
/// tests): while the first serve is parked on an in-flight upstream pull, the
/// accept loop never reaches the second connection, so the second leaf's connect
/// times out and the concurrent same-hash test fails. Mirrors the production
/// per-connection dispatch.
fn spawn_server_concurrent(
    server_ep: iroh::Endpoint,
    handler: Arc<decdn_node::handlers::client::ClientHandler>,
) -> tokio::task::JoinHandle<()> {
    use decdn_node::handlers::client::ClientProtocol;
    use iroh::protocol::ProtocolHandler;
    tokio::spawn(async move {
        while let Some(incoming) = server_ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let _ = ClientProtocol::new(handler).accept(conn).await;
            });
        }
    })
}

/// A protocol-correct upstream that serves the *right* bytes but rejects the
/// closing voucher with `StreamError(VoucherRejected { reason })` — the caller's
/// `VoucherRejectReason` — instead of accepting it: the buyer-side payment failure
/// of #857/#852. Drives the requester to `UpstreamVoucherRejected`. Modelled on
/// [`serve_wrong_bytes`] but serving the correct payload so the failure is
/// unambiguously the voucher leg, not corruption.
async fn serve_then_reject_voucher(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    served: &[u8],
    rate: u64,
    reason: VoucherRejectReason,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read request: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode request: {e}"))?
            .0
    };
    let ClientMessage::StreamRequest(req) = req_msg else {
        anyhow::bail!("voucher-rejecting upstream: expected a StreamRequest");
    };
    let body = StreamResponseBody {
        hash: req.hash,
        ok: true,
        rate_per_mb: rate,
        total_bytes: u64::try_from(served.len()).unwrap_or(u64::MAX),
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
    for chunk in served.chunks(WIRE_FRAME) {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    // Reject the closing voucher instead of acking it — the buyer's own payment
    // fault, surfaced as a mid-stream `StreamError`.
    let voucher_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode voucher: {e}"))?
            .0
    };
    // Fail fast on a protocol regression: if the buyer stops presenting a closing
    // voucher here, surface it as a clear assertion rather than a silent no-op
    // that the buyer would only see as a confusing timeout/EOF.
    let ClientMessage::Voucher(_) = voucher_msg else {
        anyhow::bail!("voucher-rejecting upstream: expected a Voucher");
    };
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamError(StreamError::VoucherRejected {
            reason,
            bundle: None,
        }))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write voucher rejection: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Serve the whole payload, read the closing voucher, then reply with an ARBITRARY
/// `StreamError` (not a `VoucherRejected`) — the shape the receive loop's voucher-slot
/// handler (`resolve_voucher_slot`) folds into `UpstreamRefused` (#1145 review, #1484).
/// Modelled on [`serve_then_reject_voucher`], differing only in the final frame: an
/// `Overloaded`/`NotFound` in reply to a voucher must be metered as a refusal, not
/// stringified into the `Unreachable` catch-all.
async fn serve_then_error_on_voucher(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    served: &[u8],
    rate: u64,
    error: StreamError,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read request: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode request: {e}"))?
            .0
    };
    let ClientMessage::StreamRequest(req) = req_msg else {
        anyhow::bail!("voucher-erroring upstream: expected a StreamRequest");
    };
    let body = StreamResponseBody {
        hash: req.hash,
        ok: true,
        rate_per_mb: rate,
        total_bytes: u64::try_from(served.len()).unwrap_or(u64::MAX),
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
    for chunk in served.chunks(WIRE_FRAME) {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    let voucher_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode voucher: {e}"))?
            .0
    };
    let ClientMessage::Voucher(_) = voucher_msg else {
        anyhow::bail!("voucher-erroring upstream: expected a Voucher");
    };
    // The non-`VoucherRejected` reply: the frame the receive loop's `resolve_voucher_slot` sees.
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamError(error))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write voucher-reply error: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Spawn A answering probes truthfully but replying to the buyer's closing voucher with
/// `error` (a non-`VoucherRejected` `StreamError`) — the ack-wait catch-all path (#1145).
fn spawn_a_voucher_erroring_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    served: Vec<u8>,
    advertised_bytes: u64,
    rate: u64,
    error: StreamError,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let served = served.clone();
            let error = error.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, advertised_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ =
                        serve_then_error_on_voucher(conn, &eth, &dom, &served, rate, error).await;
                });
            }
        }
    })
}

/// Spawn A serving probes truthfully but rejecting the buyer's voucher on the
/// client stream, for the #857 voucher-rejection exoneration test.
fn spawn_a_voucher_rejecting_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    served: Vec<u8>,
    advertised_bytes: u64,
    rate: u64,
    reason: VoucherRejectReason,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let served = served.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, advertised_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ =
                        serve_then_reject_voucher(conn, &eth, &dom, &served, rate, reason).await;
                });
            }
        }
    })
}

/// Like [`spawn_a_voucher_rejecting_server`], but counts both `cdn/probe/v1` requests and
/// `cdn/client/v1` stream attempts — the instrument for the probe-cache wedged-filter test
/// (#1223 review), which must show this provider is never streamed to again once its
/// lane wedges, however the candidate list that would have re-selected it was built.
#[allow(clippy::too_many_arguments)]
fn spawn_a_voucher_rejecting_server_with_counters(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    served: Vec<u8>,
    advertised_bytes: u64,
    rate: u64,
    reason: VoucherRejectReason,
    probes: Arc<AtomicUsize>,
    streams: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let served = served.clone();
            if conn.alpn() == ALPN_PROBE {
                let probes = Arc::clone(&probes);
                tokio::spawn(async move {
                    probes.fetch_add(1, Ordering::SeqCst);
                    let _ = answer_probe(conn, &eth, &dom, rate, advertised_bytes).await;
                });
            } else {
                let streams = Arc::clone(&streams);
                tokio::spawn(async move {
                    streams.fetch_add(1, Ordering::SeqCst);
                    let _ =
                        serve_then_reject_voucher(conn, &eth, &dom, &served, rate, reason).await;
                });
            }
        }
    })
}

/// Spawn a provider that answers probes truthfully but HARD-FAILS the client
/// stream at the transport: it accepts the connection and immediately closes it,
/// so the buyer's `open_progressive_pull` errors on connect/read rather than stalling to a
/// timeout. Models a genuine reachability failure (NOT a `PullTimeout`,
/// `UpstreamVoucherRejected`, or `HashMismatch`) — the #857 regression guard that
/// such failures must STILL score `Unreachable` and the fix did not over-exonerate.
fn spawn_a_probe_ok_client_dead_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                // Kill the client stream immediately: close the connection before
                // any `StreamResponse`, so the buyer's read fails at the transport.
                conn.close(0u32.into(), b"dead");
            }
        }
    })
}

/// Spawn a provider that answers probes truthfully but *stalls* on the client
/// stream: it accepts the bidi stream and reads the request, then never sends a
/// `StreamResponse` and holds the send side open, so the buyer's `open_progressive_pull`
/// blocks until its per-candidate `pull_timeout` fires. Models the slow-stall
/// failure shape whose outer-deadline interaction #859 fixes.
fn spawn_a_stalling_server(
    ep: iroh::Endpoint,
    s_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&s_eth);
            let dom = slash.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    // `_send` is held (not `_`) so the response stream is neither
                    // finished nor reset — the buyer keeps blocking on its read
                    // rather than seeing EOF. Read the request, then go quiet
                    // until the buyer gives up and drops the connection.
                    if let Ok((_send, mut recv)) = conn.accept_bi().await {
                        let _ = read_frame(&mut recv).await;
                        conn.closed().await;
                    }
                });
            }
        }
    })
}

/// #859: when the best-ranked candidate *stalls* (answers the probe, then never
/// serves bytes), the per-candidate `pull_timeout` must abandon it and the loop
/// must fall through to the next ranked candidate, which delivers. This proves
/// the mechanism the fix relies on — a per-candidate budget that the (separately
/// derived) outer deadline can no longer preempt.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn node_origin_pull_falls_through_a_stalled_candidate() -> Result<()> {
    let payload = vec![0xCDu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Honest node A: holds the blob; serves probe + client at `RATE`. ------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA2);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Stalling node S: quotes a CHEAPER rate so it ranks first (strictly
    //     cheaper ⇒ lower selection score ⇒ ranked #1), then stalls on the
    //     client stream. ---------------------------------------------------------
    let s_sk = fresh_key();
    let s_id = s_sk.public();
    let s_eth = Arc::new(PrivateKeySigner::random());
    let (ep_s, addr_s) =
        local_endpoint(s_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_s = spawn_a_stalling_server(
        ep_s.clone(),
        Arc::clone(&s_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    // Prime B's iroh address cache for BOTH providers (NodeId-only dialing).
    for (id, addr) in [(a_id, addr_a), (s_id, addr_s)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());

    let s_dht = DhtNodeId::from_bytes(*s_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(s_dht, s_eth.address());
    addr_map.insert(a_dht, a_eth.address());

    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    // The staller quotes the cheaper `STALL_RATE` so it ranks ahead of A (`RATE`).
    // Order is PINNED via a seeded probe cache: RTT is a multiplicative ranker term, so
    // a loaded runner's live-probe jitter could otherwise float A ahead of the staller,
    // deliver from A first, and leave the staller untried (no timeout, flake). See
    // `build_origin_seeded_ranking`.
    let (origin, engine, _engine_tmp) = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &[(s_dht, STALL_RATE), (a_dht, RATE)],
        addr_map,
        // Short per-candidate budget so the stall is abandoned quickly. With the
        // pre-#859 wiring an equal outer deadline would have cancelled the whole
        // fetch here; at the `NodeOrigin` level there is no outer wrapper, so this
        // exercises the per-candidate fallthrough the fix preserves.
        Duration::from_secs(1),
        // Generous stall budget: this fixture stalls at the OPEN stage (before the
        // `StreamResponse`), so it must be the 1 s open budget above that abandons
        // the candidate, not the streaming inactivity bound (#1134).
        Duration::from_secs(20),
    )
    .await;

    // The orchestration must abandon the staller and deliver from A.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "expected the blob from the honest fallback candidate"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "pulled bytes mismatch"
    );

    // Only ONE observation: the honest fallback's clean delivery. The staller hit
    // OUR per-candidate `pull_timeout`, which is a buyer-side deadline (a possibly
    // mis-sized local config), not evidence the provider is unreachable — so it
    // records NO local reputation observation (#857). That it was attempted at
    // all is proven by the `node_pull_timeout` counter below. The staller stays
    // at the neutral cold-start 0.5.
    anyhow::ensure!(
        (local_rep.score(s_id) - 0.5).abs() < f64::EPSILON,
        "the timed-out staller's local score must stay neutral, got {}",
        local_rep.score(s_id)
    );
    // The honest fallback A delivered cleanly, so its local score rose.
    anyhow::ensure!(
        local_rep.score(a_id) > 0.5,
        "honest provider should score a clean delivery, got {}",
        local_rep.score(a_id)
    );
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;
    assert_counter(&b_metrics, "node_pull_timeout_total", 1)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    // Symmetry with the voucher/transport tests: the timeout is a single early
    // return, so it must not also land in any sibling buyer-side bucket.
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    shutdown([task_a, task_s], [&ep_b, &ep_a, &ep_s]).await?;
    Ok(())
}

/// A wedged CHANNEL OPEN must not starve the candidate fallback loop (#1143).
///
/// This is the stage #1141/#1142 did *not* bound. Those fixed the stall once a
/// candidate accepts a QUIC connection; `open_or_reuse_pool` runs BEFORE that,
/// and was unbounded on both the buffered and window paths — so a candidate whose
/// on-chain open wedges (an unresponsive RPC endpoint, an `openChannel` tx that
/// never mines) consumed the caller's entire outer deadline, candidates #2..N were
/// never reached, and the serve path refused a blob the honest fallback held. The
/// old `PULL_THROUGH_OUTER_SLACK` doc conceded exactly this.
///
/// The wedged provider here never even gets dialled, so no server is spawned for it
/// — the hazard is entirely in the buyer's chain lane, which is also why it must
/// cost the peer no reputation: our RPC being slow says nothing about them.
///
/// The wedged open raises `PoolOpenPending` — the typed sentinel for an open that
/// outlived the caller's budget and continues in a detached task. It takes the pending
/// arm of `record_pool_open_failure`, which says our chain lane is slow, not that the
/// peer misbehaved: the loop moves on, the peer is not scored, and it does not count as
/// a pool-open FAILURE.
// multi-node fixture setup, like its siblings above; the two wedged nodes are
// deliberately named in parallel (`w_*` / `w2_*`) so the pair reads as a pair.
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn wedged_open_does_not_starve_the_candidate_loop() -> Result<()> {
    let payload = vec![0xE1u8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Honest node A: holds the blob, opens instantly, serves it. -----------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xE1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Nodes W1 and W2: quote a CHEAPER rate so they rank ahead of A, answer
    //     probes honestly, and hold the blob — but their channel opens wedge. They
    //     are perfectly good providers; our chain lane to them is what is broken.
    //
    //     TWO of them, not one, and that is the point: `MAX_PROVIDER_ATTEMPTS` is 3,
    //     so wedging two forces the honest node into the LAST attempt the loop is
    //     allowed. A single wedged candidate leaves A at slot #2 and cannot tell a
    //     loop that reaches #2 from one that reaches #3. -------------------------
    let w_sk = fresh_key();
    let w_id = w_sk.public();
    let w_eth = Arc::new(PrivateKeySigner::random());
    let (ep_w, addr_w) =
        local_endpoint(w_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // Reuse the stalling server purely as a probe responder: the pull never gets
    // far enough to open a client stream against it, because the open wedges first.
    let task_w = spawn_a_stalling_server(
        ep_w.clone(),
        Arc::clone(&w_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
    );

    let w2_sk = fresh_key();
    let w2_id = w2_sk.public();
    let w2_eth = Arc::new(PrivateKeySigner::random());
    let (ep_w2, addr_w2) =
        local_endpoint(w2_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_w2 = spawn_a_stalling_server(
        ep_w2.clone(),
        Arc::clone(&w2_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(a_id, addr_a), (w_id, addr_w), (w2_id, addr_w2)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());

    let w_dht = DhtNodeId::from_bytes(*w_id.as_bytes());
    let w2_dht = DhtNodeId::from_bytes(*w2_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(w_dht, w_eth.address());
    addr_map.insert(w2_dht, w2_eth.address());
    addr_map.insert(a_dht, a_eth.address());

    let attempted: Arc<Mutex<Vec<Address>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(WedgedOpener {
        wedged: HashSet::from([w_eth.address(), w2_eth.address()]),
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        attempted: Arc::clone(&attempted),
    }) as Arc<dyn PoolOpener>;
    // Generous: this fixture wedges at the CHANNEL-OPEN stage, so the streaming
    // inactivity bound must never be what ends a candidate here. It still has to be a
    // real value — it is a term of the outer deadline.
    let stall_budget = Duration::from_secs(20);
    let per_candidate = Duration::from_secs(2);
    // Seeded ranking, not live probes. The score multiplies rate by measured RTT, so
    // on a loaded runner a wedged node's probe RTT can inflate past A's and flip the
    // pricier honest node ahead of it — then only ONE wedged open is tried and the
    // third-attempt assertion below fails for a reason that has nothing to do with
    // the wedge. Seeding every provider at one `rtt_ms` collapses the RTT term to a
    // constant, leaving the 2x rate spread to decide the order.
    let (origin, engine, _engine_tmp) = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &[(w_dht, STALL_RATE), (w2_dht, STALL_RATE), (a_dht, RATE)],
        addr_map,
        per_candidate,
        stall_budget,
    )
    .await;

    // The loop must abandon BOTH wedged candidates on their own budgets and still
    // deliver from A — inside the outer deadline the runtime would really give it,
    // derived the way the runtime derives it rather than an arbitrary generous number.
    // Before #1143 the wedged open was unbounded, so this future never resolved at all.
    //
    // At these small budgets both of the deadline's earlier (under-counting) formulas
    // also fit, so this is not what catches a regression in the deadline ARITHMETIC —
    // `selection::outer_pull_deadline_exceeds_what_every_candidate_can_actually_cost`
    // is. What this pins is that the loop reaches the third and last candidate.
    let fetched = tokio::time::timeout(
        outer_pull_deadline(per_candidate, stall_budget),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch never returned: the wedged opens starved the loop"))?
    .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "expected the blob from the honest fallback candidate"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "pulled bytes mismatch"
    );

    // The loop genuinely reached candidate #3 — it did not merely happen to pick A
    // first. Without this, a ranking change could make the test pass while leaving
    // the starvation bug in place. A is asserted LAST (not merely present): that is
    // the slot only a loop that survived both wedged opens can reach.
    let attempted = attempted
        .lock()
        .map_err(|_| anyhow::anyhow!("attempted lock poisoned"))?
        .clone();
    anyhow::ensure!(
        attempted.len() == 3 && attempted.last() == Some(&a_eth.address()),
        "the honest candidate must be reached in the THIRD and last allowed attempt, after both \
         wedged opens are abandoned; got {attempted:?}"
    );
    anyhow::ensure!(
        attempted.iter().take(2).copied().collect::<HashSet<_>>()
            == HashSet::from([w_eth.address(), w2_eth.address()]),
        "both wedged candidates must be tried BEFORE the honest one, got {attempted:?}"
    );

    // A wedged open is OUR chain lane, not the peer's fault: neither wedged provider
    // may take a local reputation hit. This is the same exoneration
    // `PullTimeout` gets, and for the same reason.
    for (wedged_id, label) in [(w_id, "W1"), (w2_id, "W2")] {
        anyhow::ensure!(
            (local_rep.score(wedged_id) - 0.5).abs() < f64::EPSILON,
            "{label}: the wedged provider's local score must stay neutral, got {}",
            local_rep.score(wedged_id)
        );
    }
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    // Both wedged candidates took the PENDING arm of `record_pool_open_failure`,
    // not the generic channel-open-failure arm. Both arms record no reputation, so
    // without these two counters the assertions above would pass even if the typed
    // `PoolOpenPending` were never produced — the test would prove nothing about
    // the mechanism it exists for. The split also matters operationally: "pending"
    // says the node's chain lane is slower than `CHANNEL_OPEN_CALLER_BUDGET`, while
    // "failure" says the tx reverted or the wallet is under-funded.
    assert_counter(&b_metrics, "node_pull_pool_open_pending_total", 2)?;
    assert_counter(&b_metrics, "node_pull_pool_open_failures_total", 0)?;

    shutdown([task_a, task_w, task_w2], [&ep_b, &ep_a, &ep_w, &ep_w2]).await?;
    Ok(())
}

/// The open outlived the caller's budget and continues in a detached task (#1143).
#[tokio::test(flavor = "multi_thread")]
async fn a_wedged_channel_open_does_not_starve_the_candidate_loop() -> Result<()> {
    wedged_open_does_not_starve_the_candidate_loop().await
}

/// A miss with no discoverable provider degrades to a clean `NotFound`, records
/// no reputation, and bumps the no-providers counter.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_no_providers_is_clean_miss() -> Result<()> {
    let hash = Hash::new(b"nobody-has-this");
    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (origin, _engine, recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &b_metrics,
        Vec::new(),
        HashMap::new(),
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "no providers must yield NotFound"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "no pull ⇒ no voucher acked ⇒ nothing to persist (#852 guard)"
    );
    assert_counter(&b_metrics, "node_pull_no_providers_total", 1)?;
    shutdown([], [&ep_b]).await?;
    Ok(())
}

/// A discovered provider with no resolvable operator address is skipped without
/// tarnishing its reputation (payment-safety: never pay an address we can't
/// resolve / verify a `slash_sig` against). The probe succeeds, the pull is
/// skipped, the fetch is a clean `NotFound`, and no outcome is recorded.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_unresolvable_address_skips_without_scoring() -> Result<()> {
    let payload = vec![0x5Au8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    // A answers probes (has_blob) but is never paid — the resolver can't map it.
    let (ep_a, addr_a) = local_endpoint(a_sk, vec![ALPN_PROBE.to_vec()]).await?;
    let task_a = {
        let ep_a = ep_a.clone();
        let eth = Arc::clone(&a_eth);
        tokio::spawn(async move {
            while let Some(incoming) = ep_a.accept().await {
                let Ok(connecting) = incoming.accept() else {
                    continue;
                };
                let Ok(conn) = connecting.await else { continue };
                let eth = Arc::clone(&eth);
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &slash_domain(), RATE, total_bytes).await;
                });
            }
        })
    };

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    // Provider discovered, but addr_map is EMPTY → unresolvable.
    let (origin, _engine, recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &b_metrics,
        vec![DhtNodeId::from_bytes(*a_id.as_bytes())],
        HashMap::new(),
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "unresolvable provider must yield NotFound"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "skipped-before-pull ⇒ nothing persisted (#852 guard)"
    );
    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// A discovered provider that can't be probed (unreachable) is scored
/// `Unreachable` (`uptime_observed`: false), the fetch is a clean `NotFound`,
/// and the unreachable counter moves.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_probe_unreachable_is_scored() -> Result<()> {
    let hash = Hash::new(b"present-at-a-dead-peer");
    // "A" is a fresh identity with NO server — dialing it fails fast.
    let a_id = fresh_key().public();
    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let mut addr_map = HashMap::new();
    addr_map.insert(
        DhtNodeId::from_bytes(*a_id.as_bytes()),
        Address::repeat_byte(0x44),
    );
    let (origin, _engine, recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &b_metrics,
        vec![DhtNodeId::from_bytes(*a_id.as_bytes())],
        addr_map,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "an unreachable provider must yield NotFound"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "an unprobeable provider is never paid ⇒ nothing persisted (#852 guard)"
    );
    assert_counter(&b_metrics, "node_pull_unreachable_total", 1)?;
    shutdown([], [&ep_b]).await?;
    Ok(())
}

/// A dishonest upstream (valid response, wrong bytes) is detected by the
/// verifying decoder at the first chunk group and scored `Corruption`
/// (`data_correct`: false), the local score drops, the corruption counter moves,
/// and no bytes surface.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn node_origin_corruption_is_classified_and_scored() -> Result<()> {
    let honest = vec![0x11u8; 4096];
    let hash = Hash::new(&honest);
    let served = vec![0x22u8; 4096]; // same length, different content → hash mismatch
    anyhow::ensure!(Hash::new(&served) != hash, "fixtures must differ");

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let total_bytes = u64::try_from(served.len()).unwrap_or(u64::MAX);
    let task_a = spawn_a_lying_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        served,
        total_bytes,
        RATE,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, _engine, recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "corrupt delivery must not surface bytes (NotFound)"
    );
    anyhow::ensure!(
        local_rep.score(a_id) < 0.5,
        "corruption must drop the local score below neutral, got {}",
        local_rep.score(a_id)
    );
    assert_counter(&b_metrics, "node_pull_corruption_total", 1)?;

    // #852: a paid-but-corrupt delivery still advanced the upstream's accepted
    // voucher (it acked the closing voucher before we caught the hash mismatch),
    // so the buyer MUST persist that watermark — otherwise the next reuse of this
    // channel re-signs a stale voucher and is rejected. 4096 B over a 1-MiB
    // interval ⇒ one closing voucher: 4096 bytes, amount 1.
    anyhow::ensure!(
        progress_log(&recorded)? == vec![(a_eth.address(), U256::from(4096), U256::from(1))],
        "corrupt-but-paid delivery must persist its acked watermark, got {:?}",
        progress_log(&recorded)?
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #857 over the real orchestration: an upstream serves the *correct* bytes but
/// rejects the buyer's closing voucher (a stale nonce / our payment fault). The
/// pull must fail to a clean `NotFound`, and — crucially — the provider must NOT
/// be tarred: a voucher rejection is OUR payment-side fault, so no observation is
/// emitted, the local score stays neutral, and only the
/// buyer-side `node_pull_voucher_rejected` counter moves (no unreachable, no
/// corruption). Before the fix, this self-inflicted failure mapped to
/// `Outcome::Unreachable` and unfairly defamed the honest provider's local reputation.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn node_origin_voucher_rejection_does_not_tar_upstream() -> Result<()> {
    let payload = vec![0x33u8; 4096];
    let hash = Hash::new(&payload);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let task_a = spawn_a_voucher_rejecting_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload.clone(),
        total_bytes,
        RATE,
        VoucherRejectReason::AmountRegression,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, _engine, _recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a rejected voucher must not surface bytes (NotFound)"
    );
    // The provider is NOT tarred: no observation, score stays neutral at 0.5.
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "provider score must stay neutral after a voucher rejection, got {}",
        local_rep.score(a_id)
    );
    // Observability: the attempt was made and the voucher-rejected counter moved;
    // no success, no unreachable, no corruption.
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 0)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// Drive one voucher rejection end-to-end and hand back what the node did about it:
/// the channels it retired, and its metrics.
///
/// A shared fixture because the interesting thing about `VoucherRejectReason` is that
/// its variants must produce DIFFERENT behaviour, and the only honest way to show that
/// is to run the same pull against different reasons and compare.
///
/// `fetches` drives the pull that many times against the SAME origin. More than one is how
/// a caller observes suppression: a provider taken out of rotation is filtered before it is
/// probed, so a second miss must not reach it — and the counters say whether it did.
async fn pull_against_a_voucher_rejecting_upstream_n(
    reason: VoucherRejectReason,
    fetches: usize,
) -> Result<(
    Arc<Mutex<Vec<(Address, B256)>>>,
    Arc<Metrics>,
    Arc<LocalReputation>,
    iroh::PublicKey,
    B256,
)> {
    let payload = vec![0x33u8; 4096];
    let hash = Hash::new(&payload);
    let pool_id = B256::repeat_byte(0xC7);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let task_a = spawn_a_voucher_rejecting_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload.clone(),
        total_bytes,
        RATE,
        reason,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let retired = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::clone(&retired),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    // What a failed pull ANSWERS with is itself reason-dependent (#1560), and the
    // split is the fix: `BadSignature`/`WrongSigner` mean the upstream could not
    // verify a signature WE produced, so they are a fault in this node and must not
    // be answered as a `NotFound` about the content. Every other reason is a
    // statement about one channel, which leaves the blob's availability untouched —
    // those stay a clean miss.
    let is_our_signer = matches!(
        reason,
        VoucherRejectReason::BadSignature | VoucherRejectReason::WrongSigner
    );
    for _ in 0..fetches {
        let got = Origin::fetch(&origin, hash, u64::MAX).await;
        if is_our_signer {
            let err = got
                .err()
                .ok_or_else(|| anyhow::anyhow!("a signature the upstream cannot verify is OUR fault; the pull must not answer a clean miss"))?;
            anyhow::ensure!(
                !err.is_transient(),
                "a broken buyer key is not cured by retrying — the engine must see a \
                 Permanent error, or it re-runs the pull and trips the peer origin's \
                 circuit breaker on our own defect, got {err:?}"
            );
        } else {
            let got = got.map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
            anyhow::ensure!(
                matches!(got, OriginFetch::NotFound),
                "a rejected voucher must not surface bytes (NotFound)"
            );
        }
    }

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok((retired, b_metrics, local_rep, a_id, pool_id))
}

/// The single-fetch shape, which is what most of these tests want.
async fn pull_against_a_voucher_rejecting_upstream(
    reason: VoucherRejectReason,
) -> Result<(
    Arc<Mutex<Vec<(Address, B256)>>>,
    Arc<Metrics>,
    Arc<LocalReputation>,
    iroh::PublicKey,
    B256,
)> {
    pull_against_a_voucher_rejecting_upstream_n(reason, 1).await
}

/// A voucher the upstream cannot VERIFY is a broken signer in THIS node — a local
/// fault, not a payment one — and it must be routed and metered as such (#1145 review).
///
/// `BadSignature`/`WrongSigner` arrive on the same wire code as a drained deposit, and
/// that is the whole trap. They do not mean "we couldn't pay"; they mean the buyer key
/// is producing signatures nobody can verify. Every candidate will reject them, so the
/// node needs the loud `OurLocalFault` arm — the one that exists precisely so a broken
/// signer is alertable instead of being quietly attributed to payments.
///
/// The ladder made this impossible to see: it checked `UpstreamVoucherRejected` BEFORE
/// `LocalPullFault`, so a node whose signer was broken reported a `debug!` about
/// vouchers and left `node_pull_local_fault_total` at zero.
///
/// Retiring the channel would be the wrong remedy here and the test says so: the channel
/// is fine. Rotating it would burn gas on a fresh channel that the same broken key would
/// fail against just as fast.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_an_unverifiable_voucher_is_a_local_fault_not_a_payment_one() -> Result<()> {
    let (retired, metrics, local_rep, a_id, _) =
        pull_against_a_voucher_rejecting_upstream(VoucherRejectReason::BadSignature).await?;

    assert_counter(&metrics, "node_pull_local_fault_total", 1)?;
    assert_counter(&metrics, "node_pull_voucher_rejected_total", 0)?;
    anyhow::ensure!(
        retired
            .lock()
            .map_err(|_| anyhow::anyhow!("retired lock poisoned"))?
            .is_empty(),
        "a signature the upstream cannot verify says nothing about the channel — rotating it \
         would burn gas on a fresh channel the same broken key fails against just as fast"
    );
    // And still not the provider's fault: it was right to reject what we sent.
    assert_counter(&metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "provider score must stay neutral, got {}",
        local_rep.score(a_id)
    );
    Ok(())
}

/// A BUFFERED walk whose every attempt dies on our own side refuses rather than reporting
/// an absent blob, on the exit that is likelier to fire in production.
///
/// `Origin::fetch` has its own budget-exhaustion arm, and it is the one a broken node
/// actually reaches: the first cold fetch fills the probe cache, so every subsequent miss is
/// a cache HIT that spends all three attempts on our own signer and leaves through
/// `miss_answer` at the `budget == 0` short-circuit — never reaching the cold tail the other
/// tests drive. Without this, reverting that arm to `Ok(OriginFetch::NotFound)` leaves the
/// whole suite green.
///
/// `candidates` is the seeded probe-cache size, and it selects WHICH exit the walk leaves
/// through — both are driven, because they are different lines with the same job:
///
/// - `MAX_PROVIDER_ATTEMPTS` spends the whole budget and exits at the `budget == 0`
///   short-circuit, without ever reaching a fresh lookup.
/// - Fewer leaves budget over, so the walk invalidates the cache entry, falls through to a
///   discovery that finds nothing (the directory is empty for this hash), and exits at the
///   no-providers arm — which must still report the fault the CACHED walk latched. That
///   cross-walk carry is the part nothing else covers, and a partial probe-cache list is
///   routine rather than exotic: `cached_candidates` filters on the negative cache, the
///   wedged-provider map, and staker liveness.
#[tokio::test(flavor = "multi_thread")]
async fn a_buffered_walk_that_exhausts_its_budget_on_local_faults_refuses_rather_than_missing()
-> Result<()> {
    for candidates in [MAX_PROVIDER_ATTEMPTS, MAX_PROVIDER_ATTEMPTS - 1] {
        buffered_local_fault_walk(candidates).await?;
    }
    Ok(())
}

/// One run of the test above, with `candidates` seeded into the probe cache.
async fn buffered_local_fault_walk(candidates: usize) -> Result<()> {
    let payload = vec![0x5Du8; 4096];
    let hash = Hash::new(&payload);

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let mut addr_map = HashMap::new();
    let ranked: Vec<(DhtNodeId, u64)> = (0..candidates)
        .map(|_| {
            let dht = DhtNodeId::from_bytes(*fresh_key().public().as_bytes());
            addr_map.insert(dht, PrivateKeySigner::random().address());
            (dht, RATE)
        })
        .collect();

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x5D),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &ranked,
        addr_map,
        Duration::from_secs(20),
        Duration::ZERO,
    )
    .await;

    let err = tokio::time::timeout(
        Duration::from_secs(10),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch never returned"))?
    .err()
    .ok_or_else(|| {
        anyhow::anyhow!(
            "a fetch whose whole budget went to OUR faults must not answer a clean miss"
        )
    })?;
    anyhow::ensure!(
        !err.is_transient(),
        "a broken deadline config is not cured by retrying, got {err:?}"
    );
    // One fault per candidate the walk reached. With a full list that is the whole budget
    // and the fetch never leaves the cached walk; with a short one the cached walk faults,
    // the entry is invalidated, and the fall-through re-probes providers that do not answer
    // — so the second walk contributes no further faults and the `Err` above can only come
    // from the latch carrying the first walk's verdict across.
    assert_counter(
        &b_metrics,
        "node_pull_local_fault_total",
        u64::try_from(candidates).unwrap_or(u64::MAX),
    )?;

    shutdown([], [&ep_b]).await?;
    Ok(())
}

/// A local fault LATCHES; it does not abort the walk (#1560).
///
/// Candidate #1's voucher is rejected `BadSignature` — a signature WE produced that the
/// upstream cannot verify, so `OurLocalFault` — and candidate #2 then delivers the blob.
/// The fetch must succeed: the flag only ever decides the answer when NOTHING delivered, so
/// a fault reached on the way to a successful pull is invisible to the caller.
///
/// This is the boundary the alternative design would have crossed. Aborting the walk on the
/// first local fault is tempting (a broken buyer key fails on every candidate anyway, so the
/// remaining attempts look like waste) — but `OurLocalFault` also covers per-candidate
/// encode and range faults, and failing fast there throws away a blob a later candidate was
/// about to serve. The metrics assertion pins the other half: the fault is still METERED
/// even though the fetch succeeded, because an operator needs to see a node that cannot sign
/// long before it stops being able to fetch anything at all.
///
/// Order is pinned by a seeded probe cache, not a live probe: RTT is a multiplicative term
/// in the ranker, so a loaded runner could otherwise float the honest candidate first and
/// the local fault would never be reached at all (a green test that proves nothing). See
/// `build_origin_seeded_ranking`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn a_local_fault_on_one_candidate_does_not_sink_a_walk_that_still_delivers() -> Result<()> {
    const CHEAP_RATE: u64 = RATE / 2;

    let payload = vec![0x5Cu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let b_buyer = Arc::new(PrivateKeySigner::random());

    // Candidate #1 (F): quotes the cheaper rate so it ranks first, serves the bytes, then
    // rejects the closing voucher as unverifiable — our signer, not its fault.
    let f_sk = fresh_key();
    let f_id = f_sk.public();
    let f_eth = Arc::new(PrivateKeySigner::random());
    let (ep_f, addr_f) =
        local_endpoint(f_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_f = spawn_a_voucher_rejecting_server(
        ep_f.clone(),
        Arc::clone(&f_eth),
        slash_domain(),
        payload.clone(),
        total_bytes,
        CHEAP_RATE,
        VoucherRejectReason::BadSignature,
    );

    // Candidate #2 (A): the honest fallback, quoting the pricier `RATE`.
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x5C);
    let (cache_a, hash_a, tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    std::mem::forget(tmp_a);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let a_metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&a_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &a_metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(f_id, addr_f), (a_id, addr_a)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let f_dht = DhtNodeId::from_bytes(*f_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(f_dht, f_eth.address());
    addr_map.insert(a_dht, a_eth.address());

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &[(f_dht, CHEAP_RATE), (a_dht, RATE)],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
    )
    .await;

    let got = tokio::time::timeout(
        Duration::from_secs(20),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch never returned"))?
    .map_err(|e| anyhow::anyhow!("a walk that reached an honest candidate must deliver: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::AlreadyAdmitted),
        "the honest fallback held the blob; the walk must not miss"
    );
    let delivered = engine.get(hash).await?;
    anyhow::ensure!(
        delivered.as_ref() == payload.as_slice(),
        "the fallback candidate's bytes must be the ones returned"
    );
    // Both halves: the fault was seen and metered, and it did not become the answer.
    assert_counter(&b_metrics, "node_pull_local_fault_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;

    shutdown([task_a, task_f], [&ep_b, &ep_a, &ep_f]).await?;
    Ok(())
}

/// #857 regression guard (the over-exoneration direction): a provider that
/// answers the probe truthfully but then HARD-FAILS the client stream at the
/// transport (connection closed before any `StreamResponse`) is a GENUINE
/// reachability failure — not a timeout, voucher rejection, or hash mismatch — so
/// it MUST still score `Outcome::Unreachable`. This pins the boundary the fix
/// narrowed: the new exoneration arms must not swallow real transport failures.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn node_origin_transport_failure_still_scores_unreachable() -> Result<()> {
    let payload = vec![0x44u8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_probe_ok_client_dead_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, _engine, _recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a dead client stream must not surface bytes (NotFound)"
    );
    // The transport failure IS scored against the provider: an observation with
    // uptime_observed:false is emitted and the local score drops below neutral.
    anyhow::ensure!(
        local_rep.score(a_id) < 0.5,
        "a transport failure must drop the local score below neutral, got {}",
        local_rep.score(a_id)
    );
    // The fix did NOT over-exonerate: this counts as unreachable, not as one of
    // the new buyer-side buckets.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 1)?;
    assert_counter(&b_metrics, "node_pull_timeout_total", 0)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// #1088 / #1134 / #1144 — wire fixtures for the failure shapes the pull leg's
// bounds and failure classification guard against. Each test below FAILS if the
// production bound or classification it pins is removed.
// ---------------------------------------------------------------------------

/// Read the buyer's opening `StreamRequest` off a freshly-accepted client
/// stream — the first move every hand-rolled upstream below makes.
async fn read_stream_request(recv: &mut iroh::endpoint::RecvStream) -> Result<StreamRequest> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("read request: {e}"))?;
    let (msg, _) = decode_message::<ClientMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode request: {e}"))?;
    let ClientMessage::StreamRequest(req) = msg else {
        anyhow::bail!("expected a StreamRequest");
    };
    Ok(req)
}

/// Build the signed `StreamResponse` for `req`. `error: None` ⇒ an accepting
/// response (`ok: true`); `Some(e)` ⇒ a REFUSAL carrying that wire code, which
/// is the shape `StreamResponse::validate` demands (`ok == false` ⇒ exactly one
/// error) and the shape `UpstreamRefused` is built from (#1144).
///
/// The `slash_sig` is real: the buyer's `verify_response` recovers it against
/// the provider's operator address before it even looks at `ok`, so a refusal
/// that is not properly signed would fail as a bad signature and never reach the
/// classification path under test.
fn signed_response(
    req: &StreamRequest,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    rate: u64,
    total_bytes: u64,
    error: Option<StreamError>,
) -> Result<(StreamResponse, StreamResponseExt)> {
    let body = StreamResponseBody {
        hash: req.hash,
        ok: error.is_none(),
        rate_per_mb: rate,
        total_bytes,
        pool_id: req.pool_id,
        timestamp_us: req.timestamp_us,
    };
    let slash_sig = StreamSlashData::from_response_body(&body)
        .sign(eth.as_ref(), slash)
        .map_err(|e| anyhow::anyhow!("slash sign: {e}"))?
        .as_bytes()
        .to_vec();
    Ok((
        StreamResponse { body, slash_sig },
        StreamResponseExt { error },
    ))
}

/// The honest whole-blob bao verified-stream wire for `payload` (ADR 038), with
/// the 8-byte LE size header stripped — exactly the byte sequence an honest
/// upstream emits on `cdn/client/v1`. Sibling of the corrupt wire built inline by
/// `window_pull_through_mid_stream_corruption_scores_upstream_not_local`.
fn honest_bao_wire(payload: &[u8]) -> Result<Vec<u8>> {
    support::honest_bao_wire_range(payload, 0, 0)
}

/// A protocol-correct upstream that accepts the request, signs a valid
/// `StreamResponse`, and then emits an UNBOUNDED run of EMPTY `ChunkData` frames
/// (#1088).
///
/// This is the hostile shape the non-empty floor exists for, and it is bounded by
/// nothing else on either receive loop:
///
/// - the `cumulative <= expected_wire_bytes` overrun guard never trips, because
///   an empty frame advances `cumulative` by zero,
/// - the voucher cadence never fires (`bytes_since_voucher` also stays at zero),
///   so there is no round trip that could fail,
/// - and the window path's INACTIVITY deadline is re-armed per read (#1134), so
///   a frame arriving every few microseconds refreshes it forever.
///
/// `ChunkData::validate()` is therefore the ONLY thing standing between this
/// stream and an infinite spin. Frames flow until the buyer drops the connection
/// (which is what the fix makes it do, on the very first one); the write error
/// that follows ends the loop.
///
/// [`EMPTY_CHUNK_GAP`] between frames is a HARNESS concern, not a softening of
/// the attack. Written back-to-back, the frames keep the buyer's next read
/// permanently Ready, so its receive loop never yields — and a task that never
/// yields cannot be preempted by ANY timeout, including the test's own. The
/// unfixed loop then wedges the whole test binary (verified: >360 s, no result)
/// instead of failing it. Pacing the frames makes each read genuinely pend, so
/// the spin stays a spin but the test can observe it and fail. An empty frame
/// every millisecond is exactly as unbounded, and advances exactly as little.
/// Hand-forge the wire bytes of a zero-length `ChunkData` frame — the thing #1088 bans.
///
/// It cannot be built through the type any more: `ChunkData`'s field is private and both
/// `ChunkData::new` and its `#[serde(try_from)]` decode gate reject an empty payload. That
/// is precisely the fix, and it is why this helper exists rather than a struct literal: a
/// hostile peer is not a Rust caller and is not bound by our constructors. It writes
/// whatever it likes on the wire, so the adversary has to be modelled there.
///
/// Postcard encodes the payload as a length-prefixed byte slice, so a valid single-byte
/// frame is `<variant><len=1><byte>` and the empty one we want is `<variant><len=0>`.
/// Derived from a real encoding rather than a literal so it cannot drift if the variant
/// index or framing changes.
fn encode_empty_chunk_frame() -> Result<Vec<u8>> {
    let mut frame = encode_message(&ClientMessage::ChunkData(ChunkData::new(vec![0x00])?))?;
    anyhow::ensure!(
        frame.len() >= 2 && frame.pop() == Some(0x00) && frame.pop() == Some(0x01),
        "expected a one-byte ChunkData to encode as <variant><len=1><byte>; \
         the framing changed and this forgery needs rewriting"
    );
    frame.push(0x00); // len = 0
    Ok(frame)
}

async fn serve_empty_chunks(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    total_bytes: u64,
    rate: u64,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = read_stream_request(&mut recv).await?;
    let (resp, resp_ext) = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    let empty = encode_empty_chunk_frame()?;
    loop {
        tokio::time::sleep(EMPTY_CHUNK_GAP).await;
        if write_frame(&mut send, &empty).await.is_err() {
            break;
        }
    }
    Ok(())
}

/// Spawn a provider that answers probes truthfully but streams empty
/// `ChunkData` frames on the client stream (see [`serve_empty_chunks`], #1088).
fn spawn_an_empty_chunk_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_empty_chunks(conn, &eth, &dom, total_bytes, rate).await;
                });
            }
        }
    })
}

/// A provider that opens honestly — signed `StreamResponse`, then a few real
/// `ChunkData` frames — and then goes SILENT mid-stream, holding the send side
/// open forever (#1134).
///
/// This is the failure shape `PullStalled` exists to name, and it is NOT the one
/// [`spawn_a_stalling_server`] produces: that one stalls at the OPEN stage
/// (before the `StreamResponse`), which is bounded by the wall-clock open budget
/// and yields `PullTimeout` — a bound on OUR side that says nothing about the
/// peer. Here the peer has already proven it is reachable and answering, then
/// stops delivering while we wait. That is what `Unreachable` means, and the
/// inactivity deadline is the only thing that catches it.
///
/// `prefix_chunks` frames are sent (under the voucher accounting interval, so no
/// voucher round trip intrudes) against a much larger advertised `total_bytes`,
/// so the receive loop is left genuinely waiting for more.
async fn serve_then_go_silent(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    wire: Arc<Vec<u8>>,
    total_bytes: u64,
    rate: u64,
    prefix_chunks: usize,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = read_stream_request(&mut recv).await?;
    let (resp, resp_ext) = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    // A real bao prefix, not filler. The buyer decodes INCREMENTALLY and verifies
    // every chunk group against the root as it lands (ADR 038), so filler bytes
    // would be classified as CORRUPTION the moment the first parent hash failed —
    // and the pull would end on that, never reaching the silence this fixture
    // exists to produce. Honest bytes make the prefix real progress, which is what
    // the callers' doc comments already claim it is.
    for frame in wire_frames(&wire, prefix_chunks)? {
        write_frame(&mut send, &frame)
            .await
            .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    // Go quiet. `send` is held (never finished, never reset), so the buyer sees no
    // EOF and no error — only silence. Nothing but the inactivity deadline ends
    // this.
    conn.closed().await;
    Ok(())
}

/// The first `chunks` `ChunkData` frames of `wire`, each [`WIRE_FRAME`] bytes (the
/// last one short if `wire` runs out).
///
/// Shared by the go-silent fixtures so a "prefix" is always a genuine prefix of the
/// blob's bao encoding rather than filler that the buyer's incremental decoder would
/// reject as corruption before the fixture's real behaviour ever ran.
fn wire_frames(wire: &[u8], chunks: usize) -> Result<Vec<Vec<u8>>> {
    wire.chunks(WIRE_FRAME)
        .take(chunks)
        .map(|c| {
            let data = ChunkData::new(c.to_vec())?;
            Ok(encode_message(&ClientMessage::ChunkData(data))?)
        })
        .collect()
}

/// Spawn a provider that answers probes truthfully, opens the client stream
/// honestly, and then goes silent mid-delivery (see [`serve_then_go_silent`]).
fn spawn_a_mid_stream_silent_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    wire: Arc<Vec<u8>>,
    total_bytes: u64,
    rate: u64,
    prefix_chunks: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let wire = Arc::clone(&wire);
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_then_go_silent(
                        conn,
                        &eth,
                        &dom,
                        wire,
                        total_bytes,
                        rate,
                        prefix_chunks,
                    )
                    .await;
                });
            }
        }
    })
}

/// A provider that opens honestly, delivers a FULL chunk, acks the proof the
/// buyer presents for it — and only THEN goes silent (#1145 review).
///
/// The distinction from [`serve_then_go_silent`] is the whole point. That one stays
/// deliberately UNDER the voucher accounting interval, so no voucher round trip intrudes:
/// nothing is ever paid, and there is no watermark to lose. Here the upstream has
/// committed and acked a voucher before it stops — ADR 003 has the node commit
/// BEFORE it acks — so from the ack onward the payment is real and irreversible.
/// What the buyer does with the pull from that moment decides whether the money it
/// just spent is recorded or thrown away.
async fn serve_a_paid_interval_then_go_silent(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    wire: Arc<Vec<u8>>,
    total_bytes: u64,
    rate: u64,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = read_stream_request(&mut recv).await?;
    let (resp, resp_ext) = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;

    // Exactly one payment chunk's worth of bytes, so the buyer's unproved counter
    // lands precisely on the chunk boundary and it must present a proof before it
    // will take another byte. Rounded UP to a whole frame: `WIRE_FRAME` is this
    // fixture's own choice and need not divide `CHUNK_BYTES`, and a short count
    // would leave the counter below the boundary and never demand the proof.
    // Honest bao bytes, for the reason `serve_then_go_silent` records: the buyer
    // verifies each chunk group as it decodes, so filler would end the pull as
    // corruption long before the voucher round trip this fixture is built around.
    let interval_bytes = usize::try_from(CHUNK_BYTES).unwrap_or(usize::MAX);
    let interval_chunks = interval_bytes.div_ceil(WIRE_FRAME);
    for frame in wire_frames(&wire, interval_chunks)? {
        write_frame(&mut send, &frame)
            .await
            .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }

    let voucher_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode voucher: {e}"))?
            .0
    };
    let ClientMessage::Voucher(_) = voucher_msg else {
        anyhow::bail!("expected a Voucher at the interval boundary, got {voucher_msg:?}");
    };

    // Paid — and now quiet. `send` is held open (never finished, never
    // reset), so the buyer sees no EOF and no error, and keeps waiting for the rest
    // of a blob that advertised more than it got.
    conn.closed().await;
    Ok(())
}

/// Spawn a provider that answers probes truthfully, is paid for one full interval,
/// and then goes silent (see [`serve_a_paid_interval_then_go_silent`]).
fn spawn_a_paid_then_silent_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    wire: Arc<Vec<u8>>,
    total_bytes: u64,
    rate: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let wire = Arc::clone(&wire);
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_a_paid_interval_then_go_silent(
                        conn,
                        &eth,
                        &dom,
                        wire,
                        total_bytes,
                        rate,
                    )
                    .await;
                });
            }
        }
    })
}

/// A pull that is CANCELLED mid-stream must still persist the voucher watermark the
/// upstream already acked (#1145 review, #852).
///
/// Cancellation is not an exotic path here — it is the designed behaviour. The pull
/// carries `hard_cap: None`, so nothing INSIDE it ends a slow-but-progressing
/// transfer. Everything that does end one is external, and every one of them DROPS
/// the `fetch` future rather than returning through it: the foreground
/// `outer_pull_deadline`, or the serve future being dropped (client disconnect, node
/// shutdown). Dropping the future cancels the pull's `CancellationToken`, which the
/// off-thread `drive` selects on and stops.
///
/// The money is spent the instant the upstream acks, so the watermark must outlive the
/// dropped future. It does: the [`SettleOnDrop`] guard rides the PULL THREAD (not the
/// `fetch` future), so it persists the final acked watermark AFTER the cancelled
/// `drive` fully stops. Lose that — settle nothing, or settle a stale value from the
/// racing outer future — and the next pull re-signs from a stale cumulative amount, the
/// upstream rejects it `AmountRegression`, and once the bounded watermark-resume attempts
/// are spent the lane wedges and the provider drops out of ranking for the suppression
/// window.
///
/// The cancellation here is a `timeout` that drops the fetch — standing in for the real
/// droppers — against a stall budget long enough that `PullStalled` cannot be what ends
/// it. The bytes were paid for either way; the test asks only whether we wrote that
/// down, and wrote it exactly once at the acked watermark.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_cancelled_pull_still_persists_the_acked_watermark() -> Result<()> {
    // Advertise two chunks but serve only the first one, so the buyer is left
    // waiting for a remainder that never comes — with one chunk already bought
    // and acked.
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(2)).unwrap_or(usize::MAX);
    let payload = vec![0x7Du8; payload_len];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_paid_then_silent_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        Arc::new(honest_bao_wire(&payload)?),
        total_bytes,
        RATE,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0xD2),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        // A stall budget far longer than the cancellation below, so the inactivity
        // deadline provably is NOT what ends this pull. The drop is.
        Duration::from_mins(2),
        0,
    )
    .await;

    // Cancel it: `timeout` drops the fetch future exactly as the foreground
    // `outer_pull_deadline` and node shutdown do.
    let cancelled = tokio::time::timeout(
        Duration::from_secs(5),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await;
    anyhow::ensure!(
        cancelled.is_err(),
        "fixture is wrong: the pull must still have been in flight (waiting on a \
         silent-but-paid upstream) when the cancellation dropped it"
    );

    // The upstream acked one chunk before going quiet: CHUNK_BYTES of bytes,
    // `ceil(CHUNK_BYTES × RATE / 1 MiB)` in amount. That is real USDC, and it must be on the buyer's books even though
    // the pull that spent it never returned.
    //
    // The persist happens on the PULL THREAD, not on the dropped `fetch` future:
    // dropping the future cancels the token, and the pull thread then stops `drive`,
    // waits for the abandoned upstream connection to reach drained, and only then drops
    // its `SettleOnDrop` guard. So the watermark lands a beat AFTER the cancellation,
    // not synchronously with it — poll for it rather than reading once. (Were the
    // settle still on the outer future, or missing, this poll would time out: that is
    // the wedge this test guards.)
    let interval_amount = min_payment(CHUNK_BYTES, RATE);
    let want = vec![(a_eth.address(), U256::from(CHUNK_BYTES), interval_amount)];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while progress_log(&recorded)? != want {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "a cancelled pull must persist the watermark the upstream already acked, \
             from the pull thread — otherwise the next reuse re-signs from a stale \
             cumulative amount and the lane wedges, suppressing the provider. Got {:?}",
            progress_log(&recorded)?
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // And it settles EXACTLY once, at that acked watermark: the pull thread stopped the
    // instant it was cancelled (#1610), so it never advances the ledger past the acked
    // interval and never persists a second, higher (or regressed) watermark that a later
    // reuse would collide with. Give a stopped pull room to misbehave, then re-check the
    // log is still the single acked entry.
    tokio::time::sleep(Duration::from_secs(1)).await;
    anyhow::ensure!(
        progress_log(&recorded)? == want,
        "a cancelled pull must settle once at the acked watermark and not keep \
         advancing after cancel; got {:?}",
        progress_log(&recorded)?
    );

    // The abandoned upstream reached drained inside the cap, so nothing was stranded.
    // A tick here means this node dropped a per-serve runtime with a live QUIC driver
    // on it, which is what makes an endpoint close hang — the failure mode the whole
    // cancel path exists to avoid.
    //
    // This reads as "drained", not "not finished yet", only because the poll above
    // waited for the SETTLE, and the settle guard drops after the drain returns. Move
    // those two apart and this assertion goes vacuous with nothing to flag it.
    assert_counter(&b_metrics, "node_pull_abandon_drain_timeout_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// A provider that opens honestly, delivers a few real frames, and then sends a
/// `StreamError` MID-STREAM (#1145 review).
///
/// The stage is what makes this distinct. A refusal at the OPEN arrives in the
/// `StreamResponse` (`ok: false`) and has been typed since #1144. The same wire code
/// arriving *mid-delivery* took a different code path entirely — three
/// `bail!("stream failed: {e:?}")` sites that stringified it — so it fell through every
/// `downcast_ref` in `classify_pull_failure` to the catch-all and scored the peer
/// `Unreachable`. Same code, same meaning, opposite verdict, decided by nothing but which
/// frame it rode in on.
async fn serve_then_error_mid_stream(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    wire: Arc<Vec<u8>>,
    total_bytes: u64,
    rate: u64,
    error: StreamError,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = read_stream_request(&mut recv).await?;
    // An HONEST open: ok == true, signed. The peer has proven it is reachable and
    // answering — everything after this is about how it stops.
    let (resp, resp_ext) = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    // Two real frames, well under the voucher accounting interval, so no voucher round trip
    // intrudes and the loop is unambiguously mid-delivery when the error lands. Real bao
    // bytes, not filler: the buyer verifies each chunk group as it decodes, so filler
    // would end the pull as CORRUPTION before the mid-stream error this test is about
    // ever arrived (`wire_frames`).
    for frame in wire_frames(&wire, 2)? {
        write_frame(&mut send, &frame)
            .await
            .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamError(error))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write stream error: {e}"))?;
    // Flush and hold the connection until the peer closes it. Dropping `conn` here would
    // RESET the stream, and the requester would see a transport error instead of the frame
    // — which lands in the catch-all and scores `Unreachable`, i.e. it would look exactly
    // like the bug this test is here to catch, for entirely the wrong reason.
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Spawn a provider that answers probes truthfully, opens honestly, and then fails the
/// delivery with a mid-stream `StreamError` (see [`serve_then_error_mid_stream`]).
fn spawn_a_mid_stream_error_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    wire: Arc<Vec<u8>>,
    total_bytes: u64,
    rate: u64,
    error: StreamError,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let err = error.clone();
            let wire = Arc::clone(&wire);
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ =
                        serve_then_error_mid_stream(conn, &eth, &dom, wire, total_bytes, rate, err)
                            .await;
                });
            }
        }
    })
}

/// Spawn an upstream that answers probes truthfully and REFUSES the client
/// stream with `error` — a signed `StreamResponse` carrying `ok: false` plus the
/// wire code (#1144). Used for the codes a real `ClientHandler` will not produce
/// on demand (`InternalError`); the honest-`NotFound` case is driven through a
/// real handler with an empty cache, so the refusal is genuinely earned.
fn spawn_a_refusing_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
    error: StreamError,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let error = error.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_refusal(conn, &eth, &dom, total_bytes, rate, error).await;
                });
            }
        }
    })
}

/// Like [`spawn_a_refusing_server`], but counts both `cdn/probe/v1` requests and
/// `cdn/client/v1` stream attempts — the instrument for the probe-cache
/// negative-cache-interaction test (#1165), which must show this refusing
/// provider probed and streamed to exactly ONCE across two fetches.
#[allow(clippy::too_many_arguments)]
fn spawn_a_refusing_server_with_counters(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
    error: StreamError,
    probes: Arc<AtomicUsize>,
    streams: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let error = error.clone();
            if conn.alpn() == ALPN_PROBE {
                let probes = Arc::clone(&probes);
                tokio::spawn(async move {
                    probes.fetch_add(1, Ordering::SeqCst);
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                let streams = Arc::clone(&streams);
                tokio::spawn(async move {
                    streams.fetch_add(1, Ordering::SeqCst);
                    let _ = serve_refusal(conn, &eth, &dom, total_bytes, rate, error).await;
                });
            }
        }
    })
}

/// Answer the client stream with a signed refusal (`ok: false`, `error`), then
/// close — the wire shape of every `UpstreamRefused` (#1144).
async fn serve_refusal(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    total_bytes: u64,
    rate: u64,
    error: StreamError,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = read_stream_request(&mut recv).await?;
    let (resp, resp_ext) = signed_response(&req, eth, slash, rate, total_bytes, Some(error))?;
    write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write refusal: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Spawn an HONEST upstream that serves the real bao wire but SLOWLY: `gap` of
/// wall clock between consecutive `ChunkData` frames (#1134).
///
/// The one shape no other fixture produces, and the regression guard for the
/// whole bounds rewrite. [`serve_wire_paced`] paces by *chunk*, not by
/// time — it never sleeps — so before this, no test moved a transfer past
/// `pull_timeout`, and the old whole-blob deadline (which capped a node's
/// pullable blob size at roughly `pull_timeout × link speed`) could be
/// reintroduced with the suite still green.
fn spawn_a_slow_but_healthy_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    wire: Vec<u8>,
    total_bytes: u64,
    rate: u64,
    gap: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let wire = wire.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_wire_paced(conn, &eth, &dom, &wire, total_bytes, rate, gap).await;
                });
            }
        }
    })
}

/// Spacing between the empty frames of [`serve_empty_chunks`] — enough that the
/// buyer's read pends, so the receive loop remains preemptible and a wedge is
/// FAILABLE rather than merely unobservable. Three orders of magnitude below the
/// assertion window, so thousands of no-progress frames still arrive inside it.
const EMPTY_CHUNK_GAP: Duration = Duration::from_millis(1);
/// A long INACTIVITY budget for the two #1088 tests. It must be long enough that
/// it cannot rescue the loop within the assertion window below: if the stall
/// bound were what ended an empty-frame stream, these tests would pass with
/// `ChunkData::validate` reverted out of the receive loops and prove nothing.
/// (On the window path it could never rescue it at all — that deadline re-arms on
/// every read.)
const EMPTY_CHUNK_STALL_BUDGET: Duration = Duration::from_secs(30);
/// How long we allow either receive loop to make no progress before declaring the
/// spin. Comfortably under `EMPTY_CHUNK_STALL_BUDGET`, so a pass cannot be an
/// inactivity timeout in disguise; comfortably over a healthy loopback rejection,
/// which is immediate.
const EMPTY_CHUNK_ASSERT_WINDOW: Duration = Duration::from_secs(10);

/// #1088, receive loop (`UpstreamPull::next_chunk`, reached via `Origin::fetch`):
/// an upstream that streams empty `ChunkData` frames forever must be rejected AT
/// ONCE, on the frame itself.
///
/// `ChunkData::validate` has a unit test; this is the one that proves the receive
/// loop CALLS it. Drop the non-empty check and an empty frame passes every
/// other guard in the loop (see [`serve_empty_chunks`]) and the fetch spins until
/// the 30 s inactivity deadline — so the timeout below, not the `NotFound`, is
/// the assertion.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_empty_chunk_stream_is_rejected_not_spun_on() -> Result<()> {
    let payload = vec![0x5Eu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_an_empty_chunk_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0xE8),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(10),
        EMPTY_CHUNK_STALL_BUDGET,
        0,
    )
    .await;

    let got = tokio::time::timeout(
        EMPTY_CHUNK_ASSERT_WINDOW,
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "the receive loop never returned: it spun on empty ChunkData frames, \
             which advance neither the byte total nor the voucher cadence (#1088)"
        )
    })?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "an empty-chunk stream must not surface bytes (NotFound)"
    );
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;
    // The rejection came from the FRAME (`validate`), not from the inactivity
    // deadline eventually rescuing the loop — which is the whole distinction this
    // test exists to make.
    assert_counter(&b_metrics, "node_pull_stalled_total", 0)?;
    assert_counter(&b_metrics, "node_pull_success_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1797: an upstream that opens honestly and then goes SILENT mid-stream must be
/// abandoned on the THROUGHPUT FLOOR — classified as a stall, but NON-ATTRIBUTABLE.
///
/// This test separates `PullStalled` from `PullTimeout` as METRICS, and pins that
/// neither scores the peer:
///
/// - `PullTimeout` is the floor firing before the first byte — our own budget, which
///   scales with blob size, so it must not tar the peer;
///   `node_origin_pull_falls_through_a_stalled_candidate` pins that exoneration.
/// - `PullStalled` is the floor firing after bytes have flowed. Under #1797 it is
///   ALSO non-attributable: a stream that falls below the floor may be slow for
///   reasons the peer cannot be blamed for, and a throughput signal is spoofable, so
///   it is metered and the `(peer, hash)` pair is suppressed but reputation is left
///   untouched.
///
/// So the two differ only in which counter increments; the score stays neutral either
/// way.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_mid_stream_silence_does_not_score_stalled_upstream() -> Result<()> {
    // Advertise a big blob but send only a few frames, so the receive loop is left
    // genuinely waiting for the rest.
    let payload = vec![0x7Du8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // Three 1 KiB frames: real progress (so this is unambiguously a MID-stream
    // stall, not an open-stage one), but far under the voucher accounting interval, so
    // no voucher round trip intrudes on the silence that follows.
    let task_a = spawn_a_mid_stream_silent_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        Arc::new(honest_bao_wire(&payload)?),
        total_bytes,
        RATE,
        3,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0xD1),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        // A GENEROUS open budget against a SHORT stall budget — the reverse of the
        // open-stage stall test. If the two were interchangeable the peer would be
        // exonerated by the wrong bound; sizing them this way means only the
        // inactivity deadline can be what ends this pull.
        Duration::from_secs(20),
        Duration::from_secs(2),
        0,
    )
    .await;

    let got = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a mid-stream-silent upstream was never abandoned (#1134)"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a stalled delivery must not surface bytes (NotFound)"
    );

    // The stall was CLASSIFIED as a stall, not as our own deadline firing.
    assert_counter(&b_metrics, "node_pull_stalled_total", 1)?;
    assert_counter(&b_metrics, "node_pull_timeout_total", 0)?;
    // …but NOT scored (#1797): a throughput-floor abort is requester-local policy, the same
    // non-attributable class as `PullTimeout`. No `Unreachable` outcome is recorded.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    // The local score is untouched — the peer keeps its neutral default.
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < 1e-9,
        "a mid-stream stall must NOT move the local score off neutral (#1797), got {}",
        local_rep.score(a_id)
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// The mirror image of the test above, and the line between them is which METRIC the abort
/// increments: a peer that never sent a FIRST byte is classified `PullTimeout`, not
/// `PullStalled` (#1145 review, #1797). Neither scores the peer.
///
/// The split at the first byte is about attribution language, not reputation. Before the
/// first byte the throughput floor is measuring the server's TIME TO FIRST BYTE, which scales
/// with blob size, because the serve path writes the `StreamResponse` and only then
/// materialises the whole bao wire encoding (`export_bao_range`) before it can emit chunk #1.
///
/// So a 1 GiB blob — the default `max_blob_size_mb` — read off a cold disk, or served by a
/// node already streaming to several peers, can exceed the 20 s default window doing exactly
/// what it was asked. That wait is our own budget, not the peer's fault, so it counts as
/// `PullTimeout`.
///
/// Under #1797 the post-first-byte case (`PullStalled`) is ALSO non-attributable, so the two
/// verdicts differ only in which counter increments — this test pins the `PullTimeout` half,
/// its sibling above pins the `PullStalled` half, and both assert the score stays neutral.
///
/// Zero prefix chunks is the entire fixture. Its sibling above sends three; the two together
/// pin the metric boundary at exactly one byte.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_a_silent_first_byte_is_our_deadline_not_the_peers_fault() -> Result<()> {
    let payload = vec![0x7Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // ZERO chunks: a signed, honest `StreamResponse`, and then nothing — the shape of a
    // server still grinding through a large `export_bao_range`.
    let task_a = spawn_a_mid_stream_silent_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        Arc::new(honest_bao_wire(&payload)?),
        total_bytes,
        RATE,
        0,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x7E),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        // A generous OPEN budget, so the open stage is provably not what ends this: the peer
        // does answer, promptly and correctly. Only the first-chunk wait can be what fires.
        Duration::from_secs(20),
        Duration::from_secs(2),
        0,
    )
    .await;

    let got = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a silent upstream was never abandoned"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a first-byte timeout must not surface bytes (NotFound)"
    );

    // Classified as OUR deadline, not as the peer stalling.
    assert_counter(&b_metrics, "node_pull_timeout_total", 1)?;
    assert_counter(&b_metrics, "node_pull_stalled_total", 0)?;

    // Unscored, but NOT ignored (#1145 review). A SECOND miss must not reach this peer
    // again: exonerating it and doing nothing are different things, and doing nothing left
    // the cheapest griefer in the protocol unanswered — probe honestly, accept the stream,
    // send nothing, stay top-ranked, and burn a full budget on every miss forever at no
    // cost. Suppression is reputation-neutral, so it costs an honest-but-slow peer one TTL
    // and costs a silent one its free lunch.
    //
    // If the suppression is removed this counter reaches 2: the peer is re-selected and
    // re-waited-on, which is the bug.
    let again = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the second fetch never returned"))?
    .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(matches!(again, OriginFetch::NotFound), "still no bytes");
    assert_counter(&b_metrics, "node_pull_timeout_total", 1)?;

    // And still NOT scored — the assertion that matters. This peer answered honestly and
    // may simply be a slow disk with a big blob.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "the local score must stay neutral, got {}",
        local_rep.score(a_id)
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1145 review — a `StreamError` that arrives MID-STREAM must be judged by its wire code,
/// exactly as one that arrives at the open is (#1144).
///
/// The three mid-stream receive sites used to `bail!("stream failed: {e:?}")`, throwing the
/// typed code away. The resulting error matched no sentinel in `classify_pull_failure` and
/// landed in the catch-all, scoring the peer `Unreachable` — so a node that honestly
/// reported `NotFound` after an eviction race mid-delivery was punished exactly as hard as
/// a dead one. That is the bug #1144 fixed at the open stage, alive one stage downstream.
///
/// Driven through the REAL receive loop against a real upstream that opens honestly and
/// then errors, so the assertion is on what the loop actually raises. Asserting on
/// `classify_pull_failure`'s ladder alone would pass with the `bail!`s restored — the
/// classifier is not the thing that was broken.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_mid_stream_refusal_is_metered_not_scored() -> Result<()> {
    let payload = vec![0x8Bu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // `NotFound` mid-stream: the eviction-race shape. An honest answer from a reachable
    // peer, and the one whose mis-scoring #1144 was filed about.
    let task_a = spawn_a_mid_stream_error_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        Arc::new(honest_bao_wire(&payload)?),
        total_bytes,
        RATE,
        StreamError::NotFound,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x8B),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        // Both budgets generous: the error must be what ends this pull, not a deadline.
        // Sized so a regression cannot pass by accidentally timing out into an exonerating
        // `PullTimeout` arm instead.
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    let got = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a mid-stream refusal never ended the pull"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a refused delivery must not surface bytes (NotFound)"
    );

    // Classified as a REFUSAL — which is only possible if the receive loop kept the wire
    // code. With the code stringified, this counter stays 0.
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    // …and NOT as an unreachable peer. This is the assertion the bug fails: a stringified
    // mid-stream error falls to the catch-all, and the catch-all scores `Unreachable`.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_stalled_total", 0)?;

    // The local score stays untouched: the peer answered honestly.
    // The mirror of the stall test's `< 0.5`: a stall drops the score below neutral, an
    // honest refusal must not touch it.
    anyhow::ensure!(
        local_rep.score(a_id) >= 0.5,
        "an honest mid-stream refusal must leave the local score neutral, got {}",
        local_rep.score(a_id)
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1145 — a non-`VoucherRejected` `StreamError` arriving in reply to the CLOSING VOUCHER
/// lands in the receive loop's voucher-slot handler (`resolve_voucher_slot`, the optimistic
/// loop of #1484). That handler must keep the typed wire code like the three mid-stream
/// receive sites do: stringifying it lets an honest `Overloaded`/`NotFound` fall through
/// every downcast to the `Unreachable` catch-all — scoring a reachable, honestly-answering
/// peer as a dead node.
///
/// Driven through the REAL path (the server delivers the whole payload, reads the closing
/// voucher, then replies `Overloaded`), because — as the mid-stream sibling spells out — an
/// assertion against `classify_pull_failure`'s ladder would pass with the `bail!("{e:?}")`
/// restored: the classifier is not where the fault lies. `node_pull_refused_total` is
/// reachable only if the voucher-slot handler kept the wire code as `UpstreamRefused`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_an_ack_wait_refusal_is_metered_not_scored() -> Result<()> {
    // A single-bao-group payload (≤ 16 KiB): its bao wire equals the raw content, so serving raw
    // bytes matches `expected_wire_bytes` and exactly ONE closing voucher fires — after every
    // chunk has arrived. The server can then read that single voucher and reply, with no
    // mid-stream voucher racing the chunk writes into the ack wait (the #857 test's fixture size).
    let payload = vec![0x4Du8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // `Overloaded` in reply to the voucher: backpressure from a reachable peer, the code the
    // ack-wait catch-all used to stringify into `Unreachable`.
    let task_a = spawn_a_voucher_erroring_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload.clone(),
        total_bytes,
        RATE,
        StreamError::Overloaded,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x4D),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        // Generous budgets: the refusal must end this pull, not a deadline.
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    let got = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("an ack-wait refusal never ended the pull"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a refused delivery must not surface bytes (NotFound)"
    );

    // Metered as a REFUSAL — only possible if the voucher-slot handler kept the wire code
    // as `UpstreamRefused`.
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    // …and NOT as an unreachable peer. This is the assertion the 4th-site bug fails.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_stalled_total", 0)?;
    // The peer answered honestly, so its local score is untouched.
    anyhow::ensure!(
        local_rep.score(a_id) >= 0.5,
        "an honest ack-wait refusal must leave the local score neutral, got {}",
        local_rep.score(a_id)
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1145 review — a WEDGED provider must be skipped for ALL hashes until its suppression
/// window elapses, not just the one that wedged it. The `(peer, hash)` negative-cache entry the
/// wedge writes covers only the same blob for 30s; the provider-wide `wedged_providers` entry
/// (held for `WEDGED_PROVIDER_SUPPRESSION_SECS`) is what a miss for a DIFFERENT blob needs.
/// Without it the only peer-keyed SUPPRESSION is that 30s (peer, hash) entry, which does not
/// cover a different blob, so the next miss re-selects the provider and re-wedges it.
///
/// Fail-on-revert: drop the `provider_is_wedged` filter in `probe_and_rank` and the second
/// pull re-selects A, re-wedging it — `node_pull_pool_wedged_total` becomes 2, not 1.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn node_origin_a_wedged_provider_is_skipped_for_other_hashes() -> Result<()> {
    // Two distinct blobs the same provider advertises (single-bao-group, so the closing
    // voucher fires cleanly — see the ack-wait test).
    let payload1 = vec![0x11u8; 4096];
    let payload2 = vec![0x22u8; 4096];
    let hash1 = Hash::new(&payload1);
    let hash2 = Hash::new(&payload2);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // A rejects every closing voucher with `AmountRegression` → wedges on any pull it is
    // selected for. The server loops, so it handles both pulls.
    let task_a = spawn_a_voucher_rejecting_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload1.clone(),
        u64::try_from(payload1.len()).unwrap_or(u64::MAX),
        RATE,
        VoucherRejectReason::AmountRegression,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let b_dht = DhtNodeId::from_bytes(*b_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    // Probe A for BOTH hashes so it is a live, reachable candidate for each — the filter, not a
    // failed probe, must be what keeps A out of the second pull.
    for h in [&hash1, &hash2] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(a_id).with_ip_addr(addr_a),
            *h.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0xA1),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let mut addr_map = HashMap::new();
    addr_map.insert(a_dht, a_eth.address());
    let (origin, _engine, _engine_tmp) = build_origin_multi_hash(
        &ep_b,
        b_dht,
        &[hash1, hash2],
        buyer,
        &local_rep,
        &b_metrics,
        &[a_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
    )
    .await;

    // Pull 1 (hash1): A wedges its channel. One wedge event.
    let got1 = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash1, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("pull 1 never ended"))?
    .map_err(|e| anyhow::anyhow!("pull 1: {e}"))?;
    anyhow::ensure!(matches!(got1, OriginFetch::NotFound), "pull 1 must refuse");
    assert_counter(&b_metrics, "node_pull_pool_wedged_total", 1)?;

    // Pull 2 (hash2, a DIFFERENT blob): A is wedged provider-wide, so `probe_and_rank` must skip
    // it. With no other provider, the pull finds no candidate and returns NotFound WITHOUT
    // re-selecting A — so no second wedge.
    let got2 = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash2, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("pull 2 never ended"))?
    .map_err(|e| anyhow::anyhow!("pull 2: {e}"))?;
    anyhow::ensure!(
        matches!(got2, OriginFetch::NotFound),
        "pull 2 must find no candidate"
    );
    // The load-bearing assertion: still ONE wedge, not two. A re-selected-and-re-wedged
    // provider would tick this to 2 — which is exactly what dropping the filter does.
    assert_counter(&b_metrics, "node_pull_pool_wedged_total", 1)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1145 review — a `VoucherRejected` arriving MID-STREAM is the same event as one arriving
/// in reply to a voucher, and must get the same channel remedy.
///
/// The fix above (typing the mid-stream `StreamError`) created this hole one arm over. Since
/// #1484 the receive loop reads every voucher reply through one handler
/// (`resolve_voucher_slot`), which turns a `VoucherRejected` into `UpstreamVoucherRejected`
/// wherever it lands and any other `StreamError` into `UpstreamRefused` — unifying what used
/// to be a split between the blocking ack wait and the mid-stream receive sites. Before that
/// unification a `VoucherRejected` that did not arrive in a voucher round trip reached
/// `classify_refusal`, was ruled `OurFault` —
/// score nothing, suppress nothing, *do* nothing — and skipped the entire channel remedy.
///
/// The consequence is the one the drained-lane test exists to prevent, reached by another
/// road: without the wedge the next miss re-selects the same provider, and this node
/// re-presents a voucher it cannot honour on every pull — logging a `debug!` invisible at the
/// default `RUST_LOG=info`.
///
/// Driven through the REAL receive loop, for the reason the sibling test above spells out: an
/// assertion against `classify_pull_failure`'s ladder alone would pass with the bug restored,
/// because the classifier was never the thing that was broken. The counter that proves the
/// remedy ran is `node_pull_pool_wedged_total` — reachable only if the code survived the
/// receive loop AND `pull_verdict` unwrapped it back out of the refusal.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_a_mid_stream_voucher_rejection_still_reaches_the_channel_remedy() -> Result<()>
{
    let payload = vec![0x9Cu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // `InsufficientDeposit` OUTSIDE a voucher round trip: the same code the drained-channel
    // test drives through the ack wait, arriving on the other road.
    let task_a = spawn_a_mid_stream_error_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        Arc::new(honest_bao_wire(&payload)?),
        total_bytes,
        RATE,
        StreamError::VoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        },
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let retired: Arc<Mutex<Vec<(Address, B256)>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x9C),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::clone(&retired),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        // Generous, as above: the rejection must be what ends this pull, not a deadline.
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    let got = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a mid-stream voucher rejection never ended the pull"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a rejected voucher must not surface bytes (NotFound)"
    );

    // The remedy ran: this is reachable only if the receive loop kept the wire code AND
    // `pull_verdict` routed it to `voucher_verdict` rather than leaving it a bare refusal.
    // With the bug, it is `node_pull_refused_total` that ticks and this stays 0 — the
    // channel is left in the store to be handed back on every subsequent miss.
    assert_counter(&b_metrics, "node_pull_pool_wedged_total", 1)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 1)?;
    assert_counter(&b_metrics, "node_pull_refused_total", 0)?;

    // The row survives — the deposit is still escrowed (see the drained-channel test).
    anyhow::ensure!(
        retired
            .lock()
            .map_err(|_| anyhow::anyhow!("retired lock poisoned"))?
            .is_empty(),
        "an `InsufficientDeposit` channel must keep its row so the deposit can be reclaimed"
    );

    // Our payment fault, not the peer's: it is not scored.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "the provider's score must stay neutral, got {}",
        local_rep.score(a_id)
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1145 review — the deadline formula must budget the STALL stage, or a peer that goes
/// silent mid-stream starves the fallback loop exactly as a wedged channel open used to.
///
/// This is the third time the same hole has been dug. A candidate costs three sequential
/// stages — channel open, stream open, then streaming — and each time a stage was left out
/// of `outer_pull_deadline`, early candidates burned a budget the outer clock had not
/// allowed for and the loop died before reaching the last one. #859 was the missing stream
/// open; #1143 was the missing channel open; this is the missing stall window.
///
/// The existing starvation guard (`a_wedged_channel_open_does_not_starve_the_candidate_loop`)
/// cannot catch it: its candidates wedge at the channel OPEN, so they never reach the
/// streaming stage whose budget is in question. Its candidates cost
/// `CHANNEL_OPEN_CALLER_BUDGET` each; these cost `CHANNEL_OPEN + open + stall`.
///
/// # What this does and does NOT guard
///
/// It guards the BEHAVIOUR: a candidate that goes silent mid-stream is abandoned on the
/// stall bound and the loop moves on, twice over, and still delivers from candidate #3.
///
/// It does NOT pin the deadline ARITHMETIC, and saying so is the whole point of this
/// paragraph — a comment claiming otherwise would be the same false claim this review
/// round was convened to remove. Verified by experiment: reverting `outer_pull_deadline`
/// to the two-term formula leaves this test GREEN. It cannot be otherwise at any
/// affordable runtime. The reverted budget still allows `(5 + per) × 3 + 10` ≥ 28 s, so to
/// starve the loop the silent candidates must burn more than that between them — which
/// means a stall budget of ~15 s and a test that sleeps for half a minute.
///
/// The arithmetic is pinned exactly, exhaustively and instantly by
/// `selection::outer_pull_deadline_exceeds_what_every_candidate_can_actually_cost`, which
/// sweeps `per × stall` and asserts the derived deadline against an independently computed
/// worst case. That test DOES fail on revert. This one is its end-to-end companion, not
/// its substitute.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn silent_upstreams_do_not_starve_the_candidate_loop() -> Result<()> {
    // Budgets sized so the two silent candidates are abandoned on their own STALL
    // bound with room to spare under `cargo llvm-cov` instrumentation on a 4-core
    // runner — a budget too tight for that load lets them starve the outer deadline
    // before the healthy fallback is dialled, and the stall counter reads `0`. `5s`
    // keeps the shape (three sequential bounded stages per candidate) at a `77.5s`
    // outer deadline, which is what `selection::outer_pull_deadline` requires and
    // what the `.config/nextest.toml` cap for this package is sized around (#1826).
    let per_candidate = Duration::from_secs(5);
    let stall_budget = Duration::from_secs(5);

    let payload = vec![0x5Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Two candidates that open honestly, deliver 3 frames, then go silent. ---
    let s1_sk = fresh_key();
    let s1_id = s1_sk.public();
    let s1_eth = Arc::new(PrivateKeySigner::random());
    let (ep_s1, addr_s1) =
        local_endpoint(s1_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let wire = Arc::new(honest_bao_wire(&payload)?);
    let task_s1 = spawn_a_mid_stream_silent_server(
        ep_s1.clone(),
        Arc::clone(&s1_eth),
        slash_domain(),
        Arc::clone(&wire),
        total_bytes,
        STALL_RATE,
        3,
    );

    let s2_sk = fresh_key();
    let s2_id = s2_sk.public();
    let s2_eth = Arc::new(PrivateKeySigner::random());
    let (ep_s2, addr_s2) =
        local_endpoint(s2_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_s2 = spawn_a_mid_stream_silent_server(
        ep_s2.clone(),
        Arc::clone(&s2_eth),
        slash_domain(),
        Arc::clone(&wire),
        total_bytes,
        STALL_RATE,
        3,
    );

    // --- Candidate #3: healthy, and the one the loop must actually reach. -------
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xC7);
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let a_metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&a_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &a_metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: the requester. ------------------------------------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(s1_id, addr_s1), (s2_id, addr_s2), (a_id, addr_a)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let s1_dht = DhtNodeId::from_bytes(*s1_id.as_bytes());
    let s2_dht = DhtNodeId::from_bytes(*s2_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(s1_dht, s1_eth.address());
    addr_map.insert(s2_dht, s2_eth.address());
    addr_map.insert(a_dht, a_eth.address());

    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;

    let (origin, engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        vec![s1_dht, s2_dht, a_dht],
        addr_map,
        per_candidate,
        stall_budget,
        0,
    )
    .await;

    // The REAL production deadline, derived exactly as the runtime derives it. This is the
    // whole point: an assertion against a generously hand-picked number would pass with a
    // formula that cannot reach candidate #3 in production.
    let fetched = tokio::time::timeout(
        outer_pull_deadline(per_candidate, stall_budget),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "the outer deadline expired before the loop could try every candidate — the \
             stall window is a per-candidate cost and `outer_pull_deadline` must budget it"
        )
    })?
    .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;

    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "the loop gave up before reaching the healthy candidate #3 — two silent \
         candidates consumed a budget the outer deadline had not allowed for"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "wrong bytes from candidate #3"
    );

    // Both silent candidates were classified as stalls (not as our own deadline firing),
    // which is what proves they were abandoned on the STALL bound — the stage whose budget
    // this test exists to protect — rather than on some other clock. Poll for the exact
    // count to absorb the one-tick delay between the pull finishing and the metrics
    // scrape under `cargo llvm-cov` parallel load, but keep the guard strict at
    // exactly 2 — extra stalls would indicate a regression.
    {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
        let mut v = counter_value(&b_metrics, "node_pull_stalled_total")?;
        while v != 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            v = counter_value(&b_metrics, "node_pull_stalled_total")?;
        }
    }
    assert_counter(&b_metrics, "node_pull_stalled_total", 2)?;
    assert_counter(&b_metrics, "node_pull_timeout_total", 0)?;

    shutdown([task_a, task_s1, task_s2], [&ep_b, &ep_a, &ep_s1, &ep_s2]).await?;
    Ok(())
}

/// The per-candidate OPEN budget for the slow-transfer test. Small on purpose:
/// under the old whole-blob deadline this same number bounded the ENTIRE
/// exchange, so the transfer below (deliberately ~3.6 s of paced-but-healthy
/// streaming) could not have completed.
const SLOW_PULL_OPEN_BUDGET: Duration = Duration::from_secs(2);

/// #1134, the regression guard for the whole bounds rewrite: a SLOW-BUT-HEALTHY
/// transfer must COMPLETE, even though it runs well past `pull_timeout`.
///
/// The old shape wrapped the whole exchange in one wall clock, which silently
/// capped the blob size a node could pull through at roughly
/// `pull_timeout × link speed` — at the 20 s default, anything needing more than
/// ~20 s of transfer was simply unfetchable. No test caught that, because
/// `serve_wire_paced` paces by chunk and never sleeps: nothing in the
/// suite moved a transfer past the deadline at all.
///
/// So: six 1 KiB frames with a 600 ms gap ⇒ ~3.6 s of streaming, against a 2 s
/// open budget and a 5 s inactivity budget. Every individual gap is healthy (no
/// stall), the total is not (under the old deadline). Restoring
/// `PullDeadlines::whole_transfer(pull_timeout)` in `pull_from_candidate` turns
/// this into a `PullTimeout` at 2 s and a `NotFound`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_slow_but_healthy_transfer_completes_past_pull_timeout() -> Result<()> {
    // 6 KiB ⇒ six 1 KiB `ChunkData` frames (a single 16 KiB bao group, so the wire
    // is the content and one closing voucher settles it).
    let payload = vec![0x51u8; 6 * WIRE_FRAME];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let wire = honest_bao_wire(&payload)?;

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let gap = Duration::from_millis(600);
    let task_a = spawn_a_slow_but_healthy_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        wire,
        total_bytes,
        RATE,
        gap,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x51),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        SLOW_PULL_OPEN_BUDGET,
        // Comfortably above the 600 ms inter-frame gap: this upstream is slow, not
        // silent, so the inactivity bound must never fire.
        Duration::from_secs(5),
        0,
    )
    .await;

    let started = std::time::Instant::now();
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("a slow-but-healthy pull must complete, got: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "a slow-but-healthy pull returned NotFound: the transfer was killed by a \
         whole-blob deadline it should no longer have (#1134)"
    );
    let bytes = engine.get(hash).await?;
    let elapsed = started.elapsed();
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "slow-pull bytes mismatch"
    );
    // Self-check: the transfer really did outrun the old whole-blob deadline. Without
    // this the test could pass on a machine fast enough to make the pacing moot, and
    // would then guard nothing.
    anyhow::ensure!(
        elapsed > SLOW_PULL_OPEN_BUDGET,
        "the paced transfer finished in {elapsed:?}, inside the {SLOW_PULL_OPEN_BUDGET:?} \
         budget that used to bound the whole blob — this test would not detect the regression"
    );
    // No deadline fired: not the open budget (the transfer outran it, and it must
    // no longer apply), not the inactivity budget (every gap was healthy).
    assert_counter(&b_metrics, "node_pull_timeout_total", 0)?;
    assert_counter(&b_metrics, "node_pull_stalled_total", 0)?;
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1144: an honest `NotFound` refusal must NOT tar the upstream.
///
/// Candidate #1 is a REAL `ClientHandler` over an EMPTY cache — it genuinely
/// lacks the blob and earns its `NotFound` — so the refusal travels the wire as
/// the signed `ok: false` response the production serve path emits, and B
/// classifies what it actually receives. Candidate #2 holds the blob and serves
/// it.
///
/// Every refusal used to score `Outcome::Unreachable`, which punished a healthy,
/// answering node exactly as hard for truthfully saying it lacks a blob as for
/// being dead in its local reputation score. (`NotFound` is
/// NODE-scoped, not blob-scoped: seven `ServeRejectReason`s collapse onto it so
/// channel existence cannot be probed, so it is not even reliable evidence about
/// the blob.)
///
/// The `client_loopback` refusal test is NOT a guard for this: it asserts the
/// error string contains "refused", which the pre-fix `anyhow!` message satisfied
/// too. What fails on revert is the reputation half — the observation and the
/// score.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // two real serving nodes; setup dominates
async fn node_origin_not_found_refusal_does_not_tar_upstream() -> Result<()> {
    let payload = vec![0x4Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x4E);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };

    // --- Node N: a REAL handler with an EMPTY cache, quoting the cheaper rate so
    //     it ranks #1. It answers the probe (the hand-rolled responder claims the
    //     blob, exactly as an over-optimistic or just-evicted node would) and then
    //     honestly refuses on the client stream: it does not have the blob. ------
    let (cache_n, _tmp_n) = empty_cache().await?;
    let n_sk = fresh_key();
    let n_id = n_sk.public();
    let n_eth = Arc::new(PrivateKeySigner::random());
    let store_n = Arc::new(MemoryPoolStateStore::new());
    // A funded, known channel — so the refusal is unambiguously "no blob" and not
    // an unknown-channel rejection wearing the same collapsed wire code.
    store_n.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        n_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_n = Arc::new(Metrics::new());
    let handler_n = build_handler_full(
        n_id,
        &n_eth,
        &metrics_n,
        permissive_limiter(&metrics_n),
        cache_n,
        store_n as Arc<dyn PoolStateStore>,
        STALL_RATE,
        &domains,
        16,
    )?;
    let (ep_n, addr_n) =
        local_endpoint(n_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_n = spawn_a_server(
        ep_n.clone(),
        handler_n,
        Arc::clone(&n_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
    );

    // --- Node A: holds the blob and serves it. --------------------------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        permissive_limiter(&metrics_a),
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(n_id, addr_n), (a_id, addr_a)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let n_dht = DhtNodeId::from_bytes(*n_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(n_dht, n_eth.address());
    addr_map.insert(a_dht, a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        vec![n_dht, a_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    // N refuses; the loop falls through to A, which delivers.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "expected the blob from the candidate that holds it"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "pulled bytes mismatch"
    );

    // The refusal was seen and metered (so we know N really was tried, and really
    // did refuse — without this the exoneration below could pass vacuously).
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;
    // …and cost N NOTHING. This is the fix, and the two assertions that fail when
    // the `UpstreamRefused` arm is reverted to an unconditional
    // `record_outcome(Unreachable)`.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(n_id) - 0.5).abs() < f64::EPSILON,
        "an honest NotFound must leave the refusing node's local score neutral, got {}",
        local_rep.score(n_id)
    );
    // The refusal did not suppress scoring in general — A's clean delivery still
    // raised its local score; it is only NotFound that is exonerated.
    anyhow::ensure!(
        local_rep.score(a_id) > 0.5,
        "A's clean delivery must raise its local score, got {}",
        local_rep.score(a_id)
    );

    // …but exonerating N must not mean FORGETTING about it (#1145 review). N answered
    // `has_blob = true` at probe and then refused the pull, so it contradicted itself;
    // with no record of any kind it keeps its neutral score, keeps out-ranking A on
    // rate, and burns one of `MAX_PROVIDER_ATTEMPTS` on every single miss — forever.
    // The refusal is negative-cached against (N, hash), so a second fetch skips N
    // entirely: the refusal counter must NOT advance again.
    let refetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second node-origin fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(refetched, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "the second fetch must still deliver the blob from A"
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 2)?;

    shutdown([task_n, task_a], [&ep_b, &ep_n, &ep_a]).await?;
    Ok(())
}

/// #1144, the other side of the split: an `InternalError` refusal DOES score
/// `Unreachable`.
///
/// `InternalError` is the one wire code by which a node reports its OWN
/// degradation — "unexpected failure; do not retry THIS node" (#1129, the backend
/// fault / wedged-candidate signal). Exonerating every refusal would have been the
/// easy over-correction to #1144, and nothing outside the isolated `classify_refusal`
/// unit test would have noticed: this test is what makes the predicate's verdict
/// observable through the real wire + classification path.
/// The cache-wide TTL these refusal tests inject. Deliberately far BELOW
/// `REFUSAL_SUPPRESSION_TTL` (30 s), inverting the production order (5 min vs 30 s) so the
/// two suppression arms have visibly opposite lifetimes and no single implementation can
/// satisfy both assertions by accident.
const TINY_CACHE_TTL: Duration = Duration::from_millis(100);

/// Long enough that `TINY_CACHE_TTL` has certainly elapsed, short enough that
/// `REFUSAL_SUPPRESSION_TTL` certainly has not.
const PAST_THE_TINY_TTL: Duration = Duration::from_millis(600);

/// Refuse one pull with `error`, wait `wait`, then pull again — and report whether the
/// second pull was SUPPRESSED (the peer was never re-probed) or went through.
///
/// The suppression is observed behaviourally, through `cached_candidates`' filter on a
/// probe-cache hit, and `probe_and_rank`'s on the cold path — the two chokepoints every
/// candidate passes — rather than by reading the cache directly: a suppressed (peer, hash)
/// is dropped from the candidate list before it can be probed, so the second fetch never
/// becomes a second refusal. That is the property that actually matters — the cache entry
/// is only the means.
///
/// `wait` is the discriminator. At zero it asks "is this refusal suppressed AT ALL"; past
/// [`TINY_CACHE_TTL`] it asks "on whose budget". Both questions are needed: with a tiny
/// cache TTL, an arm that never suppresses and an arm whose cache-TTL suppression has
/// expired are indistinguishable after the wait, so a test that only waits cannot tell that
/// `DurableMiss`'s suppression was deleted outright.
async fn refusal_suppression_after(error: StreamError, wait: Duration) -> Result<bool> {
    let payload = vec![0x6Bu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_refusing_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        error,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x6B),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_negative_cache(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
        NegativeProbeCache::with_capacity_and_ttl(16, TINY_CACHE_TTL),
    )
    .await;

    // Pull #1: refused, and the refusal records a suppression whose TTL is the thing under
    // test.
    let _ = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;

    tokio::time::sleep(wait).await;

    // Pull #2: if the suppression is still live the candidate is filtered out before it can
    // be probed, so no second refusal is ever recorded.
    let _ = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    let text = b_metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
    let refused_twice = text.lines().any(|l| l == "decdn_node_pull_refused_total 2");

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(!refused_twice)
}

/// The two refusal suppressions must have DIFFERENT lifetimes, and this is the only test
/// that can tell (#1145 review).
///
/// `84c09dc` split them for a concrete reason: at the full 5-minute TTL, "a deposit that ran
/// dry for one pull — or the pre-observation window right after we open a channel —
/// blackholed a perfectly healthy upstream for five minutes". A `NotFound` refusal is not
/// even attributable to the peer (`ServeRejectReason::wire_error` collapses seven reasons
/// onto it, three of them ours), so it gets `REFUSAL_SUPPRESSION_TTL` — 30 s, long enough to
/// stop a retry burst re-probing a peer that just said no, short enough not to blackhole it.
/// An `EvictedSinceProbe` is a durable, honest fact about this (peer, hash), so it earns the
/// full cache TTL.
///
/// Nothing guarded the mapping. `only_a_durable_refusal_earns_the_full_suppression_ttL`
/// asserts what `classify_refusal` RETURNS and that the constant is under five minutes — but
/// not that the verdict reaches the right `suppress(...)` call. Restoring the precise bug the
/// commit was written to fix (giving `Transient` the full TTL) left the suite 1080/1080 green,
/// and removing `DurableMiss`'s suppression outright passed 38/38.
///
/// So three things are asserted, against an INVERTED cache TTL (see `TINY_CACHE_TTL`). Each
/// one is load-bearing, and dropping any of them lets one of the two mutations back in.
#[tokio::test(flavor = "multi_thread")]
async fn a_transient_refusal_is_suppressed_briefly_and_a_durable_one_for_the_full_ttl() -> Result<()>
{
    // 1. `NotFound` — transient, and maybe not even about the peer. Suppressed on its OWN
    //    fixed budget, so it OUTLIVES a (tiny) cache TTL. This is the assertion that catches
    //    the restored bug.
    anyhow::ensure!(
        refusal_suppression_after(StreamError::NotFound, PAST_THE_TINY_TTL).await?,
        "a transient refusal must be suppressed on REFUSAL_SUPPRESSION_TTL, not the cache TTL \
         — routing it through the cache-wide TTL is exactly the bug that blackholed a healthy \
         upstream for five minutes over one dry deposit"
    );

    // 2. `EvictedSinceProbe` — a durable, honest fact about this (peer, hash). It must be
    //    suppressed AT ALL. Without this, deleting the durable arm's `suppress(None)`
    //    outright is invisible: with a tiny cache TTL, "never suppressed" and "suppression
    //    already expired" look identical after any wait.
    anyhow::ensure!(
        refusal_suppression_after(StreamError::EvictedSinceProbe, Duration::ZERO).await?,
        "a durable refusal must suppress the (peer, hash) pair — otherwise a peer that \
         advertises everything and serves nothing keeps winning the ranker and burns a \
         MAX_PROVIDER_ATTEMPTS slot on every miss, forever"
    );

    // 3. …and on the CACHE-WIDE TTL, so it expires with it rather than on the transient
    //    budget. Together with (1) this pins the two arms to different calls: no single
    //    implementation satisfies both.
    anyhow::ensure!(
        !refusal_suppression_after(StreamError::EvictedSinceProbe, PAST_THE_TINY_TTL).await?,
        "a durable refusal must ride the cache-wide TTL; outliving one this long means it is \
         being given the transient budget instead"
    );
    Ok(())
}

/// Refuse one pull with `error` and report what `decdn_probe_post_eviction_failures_total`
/// reads afterwards. Modelled on [`refusal_suppression_after`], but the instrument is the
/// metric, not the suppression; asserts the refusal actually happened
/// (`node_pull_refused_total == 1`) so a returned `0` can never mean "the pull never
/// reached the classifier".
async fn post_eviction_failures_after_a_refusal(error: StreamError) -> Result<u64> {
    let payload = vec![0x4Eu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_refusing_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        error,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x4E),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a refused pull must not surface bytes"
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    let count = counter_value(&b_metrics, "probe_post_eviction_failures_total")?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(count)
}

/// `decdn_probe_post_eviction_failures_total` must fire for an `EvictedSinceProbe` refusal
/// and ONLY for it — the whole point of `DurableMissCause` (#1165, #1223 review).
///
/// The `monitoring/grafana-dashboard.json` panel scraping this metric predates the emitter,
/// and the unit test on `pull_verdict` (`only_an_eviction_carries_the_post_eviction_cause`)
/// pins the *classification*, not the emission: rerouting the metric call, or firing it for
/// both `DurableMiss` causes, passes that test while the panel silently counts blob-ceiling
/// rejections as hold-mechanism failures. This drives both causes through the real wire +
/// classification path and reads the counter itself. `BlobTooLarge` is the load-bearing
/// zero: it takes the SAME `DurableMiss` arm, so it is the one code that can tell "metric
/// keyed on the cause" from "metric keyed on the verdict".
#[tokio::test(flavor = "multi_thread")]
async fn only_an_eviction_refusal_fires_the_post_eviction_metric() -> Result<()> {
    anyhow::ensure!(
        post_eviction_failures_after_a_refusal(StreamError::EvictedSinceProbe).await? == 1,
        "an EvictedSinceProbe refusal must increment \
         decdn_probe_post_eviction_failures_total — ADR 001 §Probe cache mandates tracking \
         this rate, and the Grafana panel scraping it predates the emitter"
    );
    anyhow::ensure!(
        post_eviction_failures_after_a_refusal(StreamError::BlobTooLarge).await? == 0,
        "a BlobTooLarge refusal is a static fact about the blob, not a hold-mechanism \
         failure — counting it as one is exactly the conflation DurableMissCause exists to \
         prevent"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_origin_internal_error_refusal_scores_unreachable() -> Result<()> {
    let payload = vec![0x1Eu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_refusing_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::InternalError,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, _engine, _recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0x1E),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a refused delivery must not surface bytes (NotFound)"
    );

    // Metered as a refusal (it IS one) AND scored (this one is the peer's fault).
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 1)?;
    anyhow::ensure!(
        local_rep.score(a_id) < 0.5,
        "an InternalError refusal must drop the local score below neutral, got {}",
        local_rep.score(a_id)
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1895 over the real orchestration: an honest upstream holds and serves a blob
/// larger than B's `max_blob_size` ceiling. B does NOT refuse on the signed
/// `total_bytes` claim — it pulls, and the fill aborts once the RECEIVED bytes
/// cross the ceiling. The fetch still surfaces a clean `NotFound`, the
/// `node_pull_too_large` counter moves, and (crucially) the provider is NOT scored:
/// a buyer-side ceiling is OUR policy, not the provider's fault, so no observation
/// is emitted and its local score stays neutral. B pays the upstream for the bytes
/// it actually received before aborting (bounded to roughly one ceiling), which
/// this test does not assert on — only that the refusal is clean and unscored.
///
/// This exercises `pull_from_candidate` passing `deps.config.max_blob_size_bytes`
/// (the loopback test calls `stream_fetch_tracked` directly and bypasses it).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_oversized_blob_aborts_on_received_bytes_without_scoring() -> Result<()> {
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);
    // Buyer ceiling well below the 1.5 MiB blob → the received bytes cross it.
    let ceiling: u64 = 1_048_576;
    anyhow::ensure!(total_bytes > ceiling, "fixture must exceed the ceiling");

    // --- Node A: honest, unlimited server holding the blob. -------------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint with a sub-blob ceiling. ------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, _engine, _engine_tmp) = provisioned_origin_with_ceiling(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        pool_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        ceiling,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "an over-ceiling blob must abort the fill and surface no bytes (NotFound)"
    );
    // The provider is NOT tarred: no observation, score stays at the neutral 0.5.
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "provider score must stay neutral after a ceiling rejection, got {}",
        local_rep.score(a_id)
    );
    // Observability: the attempt was made and the too-large counter moved; no
    // success, no unreachable, no corruption.
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;
    assert_counter(&b_metrics, "node_pull_too_large_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 0)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// #1375: the buyer-side rate ceiling on the real node pull path — and specifically
/// its PROBE-RELATIVE bound, with the absolute config ceiling left unbounded (`0`).
/// Node A probes cheap (`RATE/2`) but its `ClientHandler` signs a stream quote at
/// the full `RATE` — the "quote low on the probe, quote high on the stream"
/// bait-and-switch. The buyer's effective ceiling is
/// `effective_rate_ceiling(candidate.rate = RATE/2, config = 0) = RATE/2`, so the
/// `RATE` quote is refused BEFORE any voucher, classified `PullVerdict::RateCeiling`.
///
/// This is the only test that pins `candidate.rate_per_mb` is actually bound as the
/// ceiling (the loopback test passes an explicit ceiling and bypasses
/// `effective_rate_ceiling`/`node_origin`). Like the oversized-claim sibling it
/// asserts the provider is NOT scored (buyer policy, not provider fault) and the
/// dedicated counter moves.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_over_ceiling_rate_is_rejected_without_scoring() -> Result<()> {
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);
    let probe_rate = RATE / 2; // A advertises cheap at probe...

    // --- Node A: holds the blob, probes at `probe_rate` but its handler quotes `RATE`.
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA2);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    // Handler advertises the full RATE, so its signed stream quote is RATE.
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // The probe leg answers at the LOWER probe_rate, so the candidate is selected on RATE/2.
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        probe_rate,
    );

    // --- Node B: dial-only buyer with an UNBOUNDED absolute rate ceiling (default 0).
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    // `0` ceiling => unlimited blob size, so the blob gate cannot fire; the origin's
    // NodeOriginConfig.max_rate_per_mb defaults to 0, so only the probe-relative
    // bound applies — exactly what we are exercising.
    let (origin, _engine, _engine_tmp) = provisioned_origin_with_ceiling(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        pool_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        0,
    )
    .await;

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "an over-ceiling quote must not surface bytes (NotFound)"
    );
    // Buyer policy, not provider fault: the provider's local score stays neutral.
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "provider score must stay neutral after a rate-ceiling rejection, got {}",
        local_rep.score(a_id)
    );
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;
    assert_counter(&b_metrics, "node_pull_rate_above_ceiling_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 0)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// The #852 regression: a second cache-miss pull to the same provider **reuses**
/// the buyer channel and resumes from the persisted voucher watermark, so it signs
/// cumulative amounts that continue past the first pull's rather than restarting at
/// zero, and the upstream accepts them.
///
/// Without that persistence the second pull re-signs from zero and the upstream
/// rejects it (`AmountRegression`); once the bounded watermark-resume attempts are
/// spent the second fetch is a `NotFound`. Here both fetches deliver the blob and the
/// persisted log advances monotonically — cumulative bytes and amount both double.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_reused_channel_resumes_voucher_progress() -> Result<()> {
    // Two DISTINCT blobs, same size, served by the same provider A. Gap-driven
    // `drive()` re-derives `missing_ranges` from the cache store (#1675), so a
    // second fetch of the SAME blob is already-cached and pulls/pays nothing —
    // that would starve this test of the second pull the #852 watermark-reuse
    // invariant needs. Two distinct hashes force both fetches to actually pull,
    // so both advance the shared channel's watermark.
    let payload1 = vec![0xABu8; PAYLOAD_LEN];
    let payload2 = vec![0xCDu8; PAYLOAD_LEN];
    let hash1 = Hash::new(&payload1);
    let hash2 = Hash::new(&payload2);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds both blobs; serves probe + client over one endpoint. ---
    let (cache_a, _tmp_a) = cache_with_blobs(&[&payload1, &payload2]).await?;
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for hash in [hash1, hash2] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(a_id).with_ip_addr(addr_a),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let (_providers, addr_map) = one_provider(a_dht, a_eth.address());
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_multi_hash(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        &[hash1, hash2],
        buyer,
        &local_rep,
        &b_metrics,
        &[a_dht],
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES.0,
        DEFAULT_TEST_PULL_DEADLINES.1,
    )
    .await;

    // First pull (hash1): opens the channel against A, pays for its wire bytes, and
    // persists the watermark.
    let first_fetch = Origin::fetch(&origin, hash1, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(first_fetch, OriginFetch::AlreadyAdmitted),
        "first fetch returned NotFound"
    );
    let first = engine.get(hash1).await?;
    anyhow::ensure!(
        first.as_ref() == payload1.as_slice(),
        "first pull bytes mismatch"
    );

    // Second pull (hash2, a DISTINCT blob not yet cached): REUSES the same
    // channel, resumes from the persisted watermark, and the upstream accepts
    // the continued cumulative amounts — this is the bug's fix. Fetching a distinct hash
    // (rather than re-fetching hash1) is what forces this leg to actually pull:
    // `drive()` re-derives `missing_ranges` from the cache store, so re-fetching
    // an already-cached blob would pull and pay nothing (#1675) and leave the
    // watermark untouched.
    let second_fetch = Origin::fetch(&origin, hash2, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(second_fetch, OriginFetch::AlreadyAdmitted),
        "second fetch returned NotFound — stale voucher rejected (the #852 bug)"
    );
    let second = engine.get(hash2).await?;
    anyhow::ensure!(
        second.as_ref() == payload2.as_slice(),
        "second pull bytes mismatch"
    );

    // The persisted watermark advances monotonically across the two pulls rather
    // than resetting: cumulative bytes and amount both double.
    // Under ADR 038 the pull meters WIRE bytes (bao: content + interleaved proof),
    // so each fetch contributes its bao-encoded size and per-fetch amount, and the
    // reused channel carries them forward (#852).
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(total_bytes, &bao_tree::ChunkRanges::all());
    let expected_amount = U256::from(expected_wire)
        .saturating_mul(U256::from(RATE))
        .div_ceil(U256::from(MB_BYTES));
    let log = progress_log(&recorded)?;
    anyhow::ensure!(
        log == vec![
            (a_eth.address(), U256::from(expected_wire), expected_amount),
            (
                a_eth.address(),
                U256::from(expected_wire).saturating_mul(U256::from(2)),
                expected_amount.saturating_mul(U256::from(2)),
            ),
        ],
        "expected two monotonically-advancing progress entries, got {log:?}"
    );

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// A failure to persist the voucher watermark (#852) must NOT fail the pull — the
/// bytes are already delivered and paid for — but it must surface via the
/// `node_pull_progress_persist_failures` counter so an operator can see the
/// channel is now at risk of stale-voucher rejection on its next reuse.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_persist_failure_still_delivers_and_is_counted() -> Result<()> {
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client over one endpoint. -----
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: NodeOrigin wired to an opener whose record_progress fails. ----
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let buyer = Arc::new(FailingRecordOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    // The pull delivers the verified bytes even though persisting the watermark
    // failed — the persist error must not discard already-paid-for content.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(fetched, OriginFetch::AlreadyAdmitted),
        "persist failure must not turn the pull into NotFound"
    );
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "delivered bytes mismatch"
    );

    // …but the failure is observable: the delivery still scored a clean success,
    // and the persist-failure counter moved exactly once.
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;
    assert_counter(&b_metrics, "node_pull_progress_persist_failures_total", 1)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// #856 — window-paced pull-through end-to-end (leaf → B → A).
//
// These exercise the FUSED serve path the prior tests in this file do not:
// instead of calling `Origin::fetch` directly (the buffered pull), a real leaf
// client makes a bound `cdn/client/v1` request to node B, whose `ClientHandler`
// has the window-paced provider attached. B opens a progressive pull from A,
// forwards each chunk to the leaf while teeing it into B's cache, and paces the
// upstream spend by the leaf's vouchers. The drop test is the headline #856
// regression: a leaf that drops right after the first interval must NOT make B
// front the whole blob upstream.
// ---------------------------------------------------------------------------

/// What a `leaf_paced_pull` observed.
#[derive(Debug)]
struct LeafOutcome {
    /// Decoded content bytes when the stream completes, and WIRE bytes when the
    /// leaf closes early.
    received: u64,
    acks: u64,
    /// WIRE bytes the leaf's last voucher paid for.
    paid_wire: u64,
    completed: bool,
    hash_ok: bool,
}

/// How a `leaf_paced_pull_mode` leaf pays.
#[derive(Debug, Clone, Copy)]
enum LeafMode {
    /// Pay every interval until `StreamEnd`.
    PayAll,
    /// Close the connection right after paying the n-th voucher.
    DropAfter(u64),
    /// Pay `acks` vouchers, then stay connected and keep reading without paying
    /// for `hold`, then close the connection. A `StreamEnd` inside the hold
    /// completes the pull normally.
    StopPayingAfter { acks: u64, hold: Duration },
}

/// A leaf client that requests the bounded range `[byte_offset, byte_offset +
/// byte_len)` (`byte_len == 0` ⇒ to end) through B's fused window path, pays
/// the vouchers B collects on the bao WIRE it receives, decodes + verifies the
/// aligned superset against the root, and returns exactly the requested span.
/// The ranged twin of [`leaf_paced_pull`] — the peer-path counterpart of
/// `origin_range_pull.rs`'s `ranged_paid_pull`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn leaf_ranged_paid_pull(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    provider: Address,
    pool_id: B256,
    hash: Hash,
    byte_offset: u64,
    byte_len: u64,
    rate: u64,
) -> Result<Vec<u8>> {
    use alloy::signers::SignerSync;

    let conn = leaf_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("leaf connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("leaf open_bi: {e}"))?;

    let binding_hash = binding_signing_hash(leaf_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = leaf_eth.sign_hash_sync(&binding_hash)?.as_bytes().to_vec();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
        capability: None,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id.into(),
        byte_offset,
        byte_len,
        timestamp_us: 0x9008,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write req: {e}"))?;

    let (resp, resp_ext) = read_client_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error);
    let total = resp.body.total_bytes;
    // The paid/closing boundary is the bao-encoded WIRE size of the aligned
    // superset (ADR 038), not the requested content length.
    let aligned = decdn_cache::range_pull::align_range(byte_offset, byte_len, total)
        .map_err(|e| anyhow::anyhow!("align range: {e}"))?;
    let expected_wire = decdn_cache::range_pull::bao_encoded_size(total, aligned.chunk_ranges());
    let interval_bytes = CHUNK_BYTES;

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    loop {
        match read_client(&mut recv).await? {
            ClientMessage::ChunkData(chunk) => {
                buf.extend_from_slice(chunk.bytes());
                let len = u64::try_from(chunk.bytes().len()).unwrap_or(u64::MAX);
                cumulative = cumulative.saturating_add(len);
                unvouchered = unvouchered.saturating_add(len);
                let boundary = interval_bytes > 0 && unvouchered >= interval_bytes;
                let closing = cumulative >= expected_wire && unvouchered > 0;
                if boundary || closing {
                    let amount = U256::from(cumulative)
                        .saturating_mul(U256::from(rate))
                        .div_ceil(U256::from(MB_BYTES));
                    let signed = Voucher {
                        pool_id,
                        signer: leaf_eth.address(),
                        provider,
                        amount,
                        bytes_delivered: U256::from(cumulative),
                        chain_root: B256::ZERO,
                        chunk_price: U256::ZERO,
                    }
                    .sign(leaf_eth.as_ref(), &voucher_dom())
                    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
                    write_client(
                        &mut send,
                        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)?),
                    )
                    .await?;
                    unvouchered = 0;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("leaf saw stream error: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }
    conn.close(0u32.into(), b"done");

    // Decode + verify the aligned superset against the root, trim to the span.
    let plaintext = {
        use bao_tree::BaoTree;
        use bao_tree::io::BaoContentItem;
        use bao_tree::io::sync::DecodeResponseIter;
        let tree = BaoTree::new(total, decdn_cache::range_pull::IROH_BLOCK_SIZE);
        let reader = std::io::Cursor::new(&buf[..]);
        let mut out = Vec::new();
        for item in
            DecodeResponseIter::new(hash.into(), tree, reader, aligned.chunk_ranges().as_ref())
        {
            match item.map_err(|e| anyhow::anyhow!("bao decode: {e}"))? {
                BaoContentItem::Leaf(leaf) => out.extend_from_slice(&leaf.data),
                BaoContentItem::Parent(_) => {}
            }
        }
        out
    };
    let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))
        .map_err(|e| anyhow::anyhow!("lead: {e}"))?;
    let want = if byte_len == 0 {
        plaintext.len().saturating_sub(lead)
    } else {
        usize::try_from(byte_len).map_err(|e| anyhow::anyhow!("len: {e}"))?
    };
    let end = lead.saturating_add(want);
    plaintext
        .get(lead..end)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| anyhow::anyhow!("decoded range shorter than requested span"))
}

async fn read_client(recv: &mut iroh::endpoint::RecvStream) -> Result<ClientMessage> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("leaf read frame: {e}"))?;
    let (msg, _) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("leaf decode: {e}"))?;
    Ok(msg)
}

/// Read the open-stage `StreamResponse` with its trailing extension, so a
/// refusal's wire code is available to the caller — it rides in the extension, and
/// several tests assert on it by matching the error text.
async fn read_client_response(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(StreamResponse, StreamResponseExt)> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("leaf read frame: {e}"))?;
    let (msg, tail) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("leaf decode: {e}"))?;
    let ClientMessage::StreamResponse(resp) = msg else {
        anyhow::bail!("expected StreamResponse, got {msg:?}");
    };
    let ext = decdn_protocol::parse_stream_response_ext(tail)
        .map_err(|e| anyhow::anyhow!("leaf decode ext: {e}"))?;
    Ok((resp, ext))
}

async fn write_client(send: &mut iroh::endpoint::SendStream, msg: &ClientMessage) -> Result<()> {
    let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("leaf encode: {e}"))?;
    write_frame(send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("leaf write: {e}"))?;
    Ok(())
}

/// Decode the header-less bao verified-stream `wire` (ADR 038) for the whole
/// blob back to plaintext, verifying every chunk group against `hash`. Returns
/// `None` if the stream does not verify (corrupt / short / wrong root) — the
/// same rejection a real client's decoder performs.
fn decode_bao_whole(hash: Hash, total: u64, wire: &[u8]) -> Option<Vec<u8>> {
    use bao_tree::io::BaoContentItem;
    use bao_tree::io::sync::DecodeResponseIter;
    use bao_tree::{BaoTree, ChunkRanges};

    let tree = BaoTree::new(total, decdn_cache::range_pull::IROH_BLOCK_SIZE);
    let ranges = ChunkRanges::all();
    let reader = std::io::Cursor::new(wire);
    let mut plaintext = Vec::with_capacity(usize::try_from(total).ok()?);
    for item in DecodeResponseIter::new(hash.into(), tree, reader, ranges.as_ref()) {
        match item.ok()? {
            BaoContentItem::Leaf(leaf) => plaintext.extend_from_slice(&leaf.data),
            BaoContentItem::Parent(_) => {}
        }
    }
    Some(plaintext)
}

/// A leaf client that drives B's window-paced serve: it sends a bound
/// `StreamRequest` (so B's `pull_authorized` passes), then pays one cumulative
/// voucher per interval as bytes arrive. With `drop_after_acks = Some(n)` it
/// closes the connection immediately after paying the n-th voucher — the #856
/// abandon shape. The wire carries the bao verified-stream (content + proof,
/// ADR 038), so it paces on the bao-encoded WIRE size and decodes the buffer
/// back to plaintext to verify the content hash.
#[allow(clippy::too_many_arguments)]
async fn leaf_paced_pull(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    provider: Address,
    pool_id: B256,
    hash: Hash,
    rate: u64,
    drop_after_acks: Option<u64>,
) -> Result<LeafOutcome> {
    let mode = drop_after_acks.map_or(LeafMode::PayAll, LeafMode::DropAfter);
    leaf_paced_pull_mode(
        leaf_ep,
        target,
        leaf_node_id,
        leaf_eth,
        provider,
        pool_id,
        hash,
        rate,
        mode,
    )
    .await
}

/// [`leaf_paced_pull`] with an explicit [`LeafMode`]. In
/// [`LeafMode::StopPayingAfter`] the leaf keeps reading after its last voucher
/// until `hold` elapses, then closes and reports the WIRE bytes it received. A
/// stream error or read failure during the hold is an error, and a `StreamEnd`
/// completes the pull.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn leaf_paced_pull_mode(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    provider: Address,
    pool_id: B256,
    hash: Hash,
    rate: u64,
    mode: LeafMode,
) -> Result<LeafOutcome> {
    use alloy::signers::SignerSync;

    let conn = leaf_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("leaf connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("leaf open_bi: {e}"))?;

    let binding_hash = binding_signing_hash(leaf_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = leaf_eth.sign_hash_sync(&binding_hash)?.as_bytes().to_vec();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
        capability: None,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x9001,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write req: {e}"))?;

    let (resp, resp_ext) = read_client_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error);
    let total = resp.body.total_bytes;
    // B forwards + meters WIRE bytes (bao: content + interleaved proof), so the
    // closing-voucher / completeness boundary is the bao-encoded size, not the
    // content `total_bytes`. The per-interval boundary is unchanged — both sides
    // count the same forwarded wire bytes into `interval_bytes`.
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(total, &bao_tree::ChunkRanges::all());
    let interval_bytes = CHUNK_BYTES;

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    let mut acks: u64 = 0;
    let mut paid_wire: u64 = 0;
    // Set once a `StopPayingAfter` leaf has paid its last voucher.
    let mut hold_until: Option<tokio::time::Instant> = None;
    loop {
        let msg = match hold_until {
            None => read_client(&mut recv).await?,
            Some(deadline) => match tokio::time::timeout_at(deadline, read_client(&mut recv)).await
            {
                // An early stream error or read failure means B dropped the
                // unpaid leaf, which this mode exists to catch.
                Ok(Err(e)) => anyhow::bail!("leaf read failed during the unpaid hold: {e}"),
                Ok(Ok(ClientMessage::StreamError(e))) => {
                    anyhow::bail!("B ended the stream during the unpaid hold: {e:?}")
                }
                Err(_) => {
                    conn.close(0u32.into(), b"leaf-hold");
                    return Ok(LeafOutcome {
                        received: cumulative,
                        acks,
                        paid_wire,
                        completed: false,
                        hash_ok: false,
                    });
                }
                Ok(Ok(msg)) => msg,
            },
        };
        match msg {
            ClientMessage::ChunkData(chunk) => {
                buf.extend_from_slice(chunk.bytes());
                let len = chunk.bytes().len() as u64;
                cumulative = cumulative.saturating_add(len);
                unvouchered = unvouchered.saturating_add(len);
                let boundary = unvouchered >= interval_bytes && interval_bytes > 0;
                let closing = cumulative >= expected_wire && unvouchered > 0;
                if (boundary || closing) && hold_until.is_none() {
                    acks += 1;
                    let amount = U256::from(cumulative)
                        .saturating_mul(U256::from(rate))
                        .div_ceil(U256::from(MB_BYTES));
                    let signed = Voucher {
                        pool_id,
                        signer: leaf_eth.address(),
                        provider,
                        amount,
                        bytes_delivered: U256::from(cumulative),
                        chain_root: B256::ZERO,
                        chunk_price: U256::ZERO,
                    }
                    .sign(leaf_eth.as_ref(), &voucher_dom())
                    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
                    write_client(
                        &mut send,
                        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)?),
                    )
                    .await?;
                    // Acceptance is implicit (ADR 005): no ack is read; a rejection
                    // would arrive as a mid-stream `StreamError`.
                    unvouchered = 0;
                    paid_wire = cumulative;
                    match mode {
                        LeafMode::DropAfter(n) if n == acks => {
                            conn.close(0u32.into(), b"leaf-drop");
                            return Ok(LeafOutcome {
                                received: cumulative,
                                acks,
                                paid_wire,
                                completed: false,
                                hash_ok: false,
                            });
                        }
                        LeafMode::StopPayingAfter { acks: n, hold } if n == acks => {
                            hold_until = Some(tokio::time::Instant::now() + hold);
                        }
                        _ => {}
                    }
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("leaf saw stream error: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }
    conn.close(0u32.into(), b"done");
    // Decode the accumulated bao wire back to plaintext to verify the content
    // hash; `received` reports the decoded CONTENT length (what the callers'
    // `received == total_bytes` assertions expect), falling back to the raw wire
    // count if the stream did not verify.
    let decoded = decode_bao_whole(hash, total, &buf);
    Ok(LeafOutcome {
        received: decoded.as_ref().map_or(cumulative, |p| p.len() as u64),
        acks,
        paid_wire,
        completed: true,
        hash_ok: decoded.is_some_and(|p| Hash::new(&p) == hash),
    })
}

/// Build node B: an empty-cache `ClientHandler` with the window-paced provider
/// (pointing at upstream `providers`) and the leaf's channel registered. Returns
/// the handler, B's dial target, B's endpoint, and the buyer-progress log.
#[allow(clippy::too_many_arguments)]
async fn build_node_b(
    a_id: iroh::PublicKey,
    a_addr: std::net::SocketAddr,
    a_eth_addr: Address,
    hash: Hash,
    ab_channel_id: B256,
    b_buyer: &Arc<PrivateKeySigner>,
    leaf_channel_id: B256,
    leaf_eth_addr: Address,
    leaf_deposit: U256,
    max_blob_size_bytes: u64,
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Arc<Metrics>,
    Arc<LocalReputation>,
    Address,
)> {
    build_node_b_with_leaves(
        a_id,
        a_addr,
        a_eth_addr,
        hash,
        ab_channel_id,
        b_buyer,
        &[(leaf_channel_id, leaf_eth_addr, leaf_eth_addr, leaf_deposit)],
        max_blob_size_bytes,
        64,
        None,
        DEFAULT_TEST_PULL_DEADLINES,
    )
    .await
}

/// Like [`build_node_b`] but registers an arbitrary set of leaf channels in B's
/// store, so a test can drive multiple concurrent leaf requests against the same
/// node B (each leaf needs its own channel to avoid sharing voucher state). The
/// single-leaf [`build_node_b`] is a thin wrapper over this.
///
/// `engine_max_blob_mb` caps B's cache-engine store (`CacheEngine::open`'s
/// `max_blob_mb`). It is independent of the handler's `max_blob_size_bytes`
/// (which gates the *serve*): setting the engine cap below the blob size while
/// leaving the handler cap permissive lets a test force `tee.finish()` to reject
/// the promote on an otherwise-successful delivery (#896). Most callers pass the
/// default `64`.
///
/// Each leaf is `(pool_id, funder, voucher_signer, deposit)`. The two address
/// legs are distinct on purpose: an on-chain `openChannel` may pin a delegate
/// `voucher_signer` that is not the funder, and the ADR 011 compliance gates key
/// on the FUNDER. Passing the same address twice is the undelegated default.
///
/// `content_deny` wires B's ADR 011 deny-set. `None` means "deny nothing" (the
/// steady state for every other caller); a shared `Arc` lets a test flip an entry
/// on mid-stream.
///
/// `node_pull_deadlines` is B's own upstream `(pull_timeout, stall_timeout)`. Pass
/// [`DEFAULT_TEST_PULL_DEADLINES`] unless the test is about the deadline gate itself — see
/// [`provisioned_origin_with_deadlines`] for the one case that is.
#[allow(clippy::too_many_arguments)]
async fn build_node_b_with_leaves(
    a_id: iroh::PublicKey,
    a_addr: std::net::SocketAddr,
    a_eth_addr: Address,
    hash: Hash,
    ab_channel_id: B256,
    b_buyer: &Arc<PrivateKeySigner>,
    leaves: &[(B256, Address, Address, U256)],
    max_blob_size_bytes: u64,
    engine_max_blob_mb: u64,
    content_deny: Option<Arc<decdn_node::content_deny::ContentDenylist>>,
    node_pull_deadlines: (Duration, Duration),
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Arc<Metrics>,
    Arc<LocalReputation>,
    Address,
)> {
    build_node_b_with_store(
        a_id,
        a_addr,
        a_eth_addr,
        hash,
        ab_channel_id,
        b_buyer,
        leaves,
        max_blob_size_bytes,
        engine_max_blob_mb,
        content_deny,
        node_pull_deadlines,
        |store| Arc::new(store) as Arc<dyn PoolStateStore>,
    )
    .await
}

/// Like [`build_node_b_with_leaves`], but `wrap_store` wraps B's seeded lane
/// store before the handler takes it, so a test can inject a store fault on the
/// leaf-facing lane.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn build_node_b_with_store(
    a_id: iroh::PublicKey,
    a_addr: std::net::SocketAddr,
    a_eth_addr: Address,
    hash: Hash,
    ab_channel_id: B256,
    b_buyer: &Arc<PrivateKeySigner>,
    leaves: &[(B256, Address, Address, U256)],
    max_blob_size_bytes: u64,
    engine_max_blob_mb: u64,
    content_deny: Option<Arc<decdn_node::content_deny::ContentDenylist>>,
    node_pull_deadlines: (Duration, Duration),
    wrap_store: impl FnOnce(MemoryPoolStateStore) -> Arc<dyn PoolStateStore>,
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Arc<Metrics>,
    Arc<LocalReputation>,
    Address,
)> {
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let b_eth = Arc::new(PrivateKeySigner::random());
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    // Prime B's iroh address cache for A so NodeId-only dialing in the pull
    // resolves (same priming the buffered tests use).
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(a_addr),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) = one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth_addr);
    let (origin, _engine, recorded, _engine_tmp) = provisioned_origin_with_deadlines(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        ab_channel_id,
        b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        node_pull_deadlines,
        max_blob_size_bytes,
    )
    .await;

    // B's empty cache (the tee fills it) and the leaf's channel in B's store.
    let cache_tmp = tempfile::tempdir()?;
    let cache_b =
        decdn_cache::CacheEngine::open(cache_tmp.path(), vec![], engine_max_blob_mb).await?;
    let cache_handle = cache_b.clone();
    // Leak the tempdir guard for the test's lifetime (kept alive by the returned
    // engine's open store anyway).
    std::mem::forget(cache_tmp);
    let store_b = MemoryPoolStateStore::new();
    // Each leaf's seller-side lane is keyed by `(pool_id, voucher_signer, this
    // operator)` — B's own operator address is the provider leg (dispatch.rs
    // resolves it from `self.eth_signer`), so the seeded lane must name `b_eth`,
    // never the leaf's own key. The stub pool-view maps each pool to its funder
    // (the ADR 011 subject, `getPool.owner`) and its deposit (the floor-`M`
    // solvency `remaining`).
    let mut pool_status_map: HashMap<B256, decdn_node::pool_view::PoolStatus> = HashMap::new();
    for (leaf_channel_id, leaf_funder, leaf_voucher_signer, leaf_deposit) in leaves {
        store_b.record(&LaneState::hydrate(
            *leaf_channel_id,
            *leaf_voucher_signer,
            b_eth.address(),
            *leaf_deposit,
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        ))?;
        pool_status_map.insert(
            *leaf_channel_id,
            decdn_node::pool_view::PoolStatus {
                owner: *leaf_funder,
                remaining: *leaf_deposit,
                lifecycle: decdn_node::pool_view::Lifecycle::Open,
            },
        );
    }
    let pool_view = Arc::new(StubPoolView {
        status: pool_status_map,
    }) as Arc<dyn decdn_node::pool_view::PoolView>;
    let limiter = permissive_limiter(&b_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_b = build_handler_full_configured(
        b_id,
        &b_eth,
        &b_metrics,
        limiter,
        cache_b,
        wrap_store(store_b),
        RATE,
        &domains,
        16,
        |deps| {
            // Window-paced pull-through: the deadline accommodates the full
            // discover→probe→pull, paced by the ramped credit window (#1669).
            deps.pull_through = Some(Duration::from_secs(20));
            deps.pull_through_origin = Some(Arc::new(origin));
            deps.pool_view = Some(pool_view);
            if let Some(deny) = content_deny {
                deps.content_deny = deny;
            }
        },
    )?;

    let target = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    Ok((
        handler_b,
        target,
        ep_b,
        recorded,
        cache_handle,
        b_metrics,
        local_rep,
        b_eth.address(),
    ))
}

/// Spin up A (holds the blob, serves probe + client). Returns the pieces the
/// caller needs to build B and run the leaf.
async fn spawn_node_a(
    payload: &[u8],
    ab_channel_id: B256,
    b_buyer_addr: Address,
) -> Result<(
    iroh::PublicKey,
    std::net::SocketAddr,
    Arc<PrivateKeySigner>,
    iroh::Endpoint,
    tokio::task::JoinHandle<()>,
)> {
    let (a_id, addr_a, a_eth, ep_a, task_a, _metrics) =
        spawn_node_a_metered(payload, ab_channel_id, b_buyer_addr).await?;
    Ok((a_id, addr_a, a_eth, ep_a, task_a))
}

/// [`spawn_node_a`], also returning A's metrics, so a test can count the paid
/// streams A served.
async fn spawn_node_a_metered(
    payload: &[u8],
    ab_channel_id: B256,
    b_buyer_addr: Address,
) -> Result<(
    iroh::PublicKey,
    std::net::SocketAddr,
    Arc<PrivateKeySigner>,
    iroh::Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<Metrics>,
)> {
    let hash = Hash::new(payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let (cache_a, hash_a, tmp_a) = cache_with_blob(payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    std::mem::forget(tmp_a);
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        ab_channel_id,
        b_buyer_addr,
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );
    Ok((a_id, addr_a, a_eth, ep_a, task_a, metrics))
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_serves_and_caches_full_blob() -> Result<()> {
    // 1.5x the pacing window (which floors at `PULL_WINDOW_FLOOR`, ADR 003
    // §Credit window), so the pull crosses exactly one window boundary and pauses
    // once, with a real remainder left to resume. The window — not `CHUNK_BYTES` —
    // is what sets the span boundary: the pull leg paces in CONTENT while the
    // client pays in WIRE, so its floor clears a chunk by both group roundings that
    // separate the two.
    let window = decdn_client::PULL_WINDOW_FLOOR;
    let payload_len = usize::try_from(window.saturating_mul(3) / 2).unwrap_or(usize::MAX);
    let payload = vec![0xCDu8; payload_len];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA1);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x1F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let outcome = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await?;

    anyhow::ensure!(outcome.completed, "leaf delivery did not complete");
    anyhow::ensure!(outcome.hash_ok, "leaf received bytes failed the hash check");
    anyhow::ensure!(
        outcome.received == total_bytes,
        "leaf received {} of {total_bytes} bytes",
        outcome.received
    );
    // The headline #856 behavior: the payload (1.5x the pacing window) exceeds the
    // default window, so the pull MUST have paused at the window frontier and
    // resumed as the leaf's vouchers cleared. Assert the pause actually fired —
    // a regression that broke the resume could still pass the full-delivery
    // checks above (the blob would simply never arrive), so pin the pause
    // explicitly rather than only implicitly.
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_window_paused_total")? >= 1,
        "the window pause/resume path must have engaged for a blob larger than the window"
    );
    // B cached the (verified) blob — it is now a holder for future requests.
    anyhow::ensure!(
        cache_b.has(hash).await?,
        "B must promote the teed blob on a complete delivery"
    );
    // B's buyer channel to A advanced under the DECOUPLED window-paced cadence (#1621),
    // which is not a single-open shape: the pull leg opens the blob in more than one
    // span, because the window pause at the frontier (proven above) forces a second
    // upstream open, each with its OWN voucher accounting starting fresh at that open
    // (#856) rather than continuing the cumulative total. The wire total re-emits the
    // span boundary's bao parent once more than a single-span encoding would (ADR 038
    // meters WIRE: content + interleaved proof), and each leg's own wire is chunked and
    // ceiling-rounded independently, so the two legs' summed payment is more than a
    // single cumulative ceiling over the whole wire would be.
    let leg1_wire =
        u64::try_from(honest_bao_wire_range(&payload, 0, window)?.len()).unwrap_or(u64::MAX);
    let leg2_wire =
        u64::try_from(honest_bao_wire_range(&payload, window, 0)?.len()).unwrap_or(u64::MAX);
    let expected_wire = leg1_wire.saturating_add(leg2_wire);
    let per_leg_amount = |wire: u64| -> U256 {
        let mut amount = U256::ZERO;
        let mut remaining = wire;
        while remaining > 0 {
            let chunk = remaining.min(CHUNK_BYTES);
            amount += min_payment(chunk, RATE);
            remaining -= chunk;
        }
        amount
    };
    let expected_amount = per_leg_amount(leg1_wire) + per_leg_amount(leg2_wire);
    anyhow::ensure!(
        progress_log(&recorded)?
            == vec![(a_eth.address(), U256::from(expected_wire), expected_amount)],
        "expected B's upstream watermark at the full blob ({expected_wire} wire bytes, \
         {expected_amount} paid), got {:?}",
        progress_log(&recorded)?
    );

    assert_relay_counted(&b_metrics, u64::try_from(payload.len())?).await?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// The serve-miss handshake is the pull leg's first leg (#2063). A's probe
/// reports the blob's size, so B's handshake opens `[0, window)` (the first leg
/// its paced pull draws) instead of a whole-blob open it would drop, and the pull
/// leg adopts it. A therefore serves exactly the two paid legs, `[0, window)`
/// and `[window, end)`, and no throwaway open.
#[tokio::test]
async fn window_pull_through_handshake_is_the_first_pull_leg() -> Result<()> {
    let window = decdn_client::PULL_WINDOW_FLOOR;
    let payload_len = usize::try_from(window.saturating_mul(3) / 2).unwrap_or(usize::MAX);
    let payload = vec![0x5Au8; payload_len];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA1);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a, a_metrics) =
        spawn_node_a_metered(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x1F);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let outcome = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await?;
    anyhow::ensure!(
        outcome.completed && outcome.hash_ok && outcome.received == total_bytes,
        "leaf delivery must complete byte-exact: {outcome:?}"
    );
    let served = counter_value(&a_metrics, "serve_cache_hit_total")?;
    anyhow::ensure!(
        served == 2,
        "A must serve only the two paid legs, the first of them the handshake's: served {served}"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A multi-MB blob completes on the fused serve-miss path once both credit windows
/// have left their floors and ramp with payment (#1893).
///
/// The serve leg ramps its window on paid WIRE bytes; the pull leg ramps on the
/// paid CONTENT frontier, which is always smaller. Past the floor, the pull window
/// therefore closes while the serve window still has room, and the serve encoder
/// waits on a leaf or a proof node that only the pull can fetch — while it waits,
/// it collects no voucher. The pull must still fetch the span the encoder waits on,
/// or neither leg ever moves again. At 8 MiB and the default ramp divisor, several
/// MiB of the delivery run in the ramp regime.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_completes_a_blob_past_the_ramp_floor() -> Result<()> {
    let payload_len: usize = 8 * 1024 * 1024;
    let mut payload = vec![0u8; payload_len];
    let mut x: u32 = 0x2545_f491;
    for b in &mut payload {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA3);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x3F);
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let outcome = tokio::time::timeout(
        Duration::from_mins(1),
        leaf_paced_pull(
            &leaf_ep,
            b_target,
            leaf_node_id,
            &leaf_eth,
            b_operator,
            leaf_channel_id,
            hash,
            RATE,
            None,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fused serve-miss delivery stalled"))??;

    anyhow::ensure!(outcome.completed, "leaf delivery did not complete");
    anyhow::ensure!(outcome.hash_ok, "leaf received bytes failed the hash check");
    anyhow::ensure!(
        outcome.received == total_bytes,
        "leaf received {} of {total_bytes} bytes",
        outcome.received
    );
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_window_paused_total")? >= 1,
        "the pull must have paused on its window for a blob this far past the floor"
    );
    anyhow::ensure!(
        cache_b.has(hash).await?,
        "B must promote the teed blob on a complete delivery"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// COMPLIANCE REGRESSION GUARD (ADR 011 §On Blacklist Event), WINDOW half.
///
/// `client_loopback.rs`'s `blacklisting_the_funder_mid_stream_cuts_off_a_
/// delegated_delivery` pins the same property on the buffered `delivery.rs`
/// path, but it structurally cannot reach this one: its harness wires no
/// `pull_through_origin`, and `serve_via_window_pull_through` is only entered
/// when that provider is `Some`. So the window loop's per-interval re-check —
/// which reads the funder ONCE at the top of the serve and re-consults it after
/// every accepted voucher — was guarded by a comment and nothing else.
///
/// Here the leaf's channel is funded by one address and signed by a DIFFERENT,
/// never-blacklisted delegate. Nothing is denied at open time, so the request is
/// admitted, B starts fusing the upstream pull with downstream delivery, and only
/// then does the FUNDER go on the deny-set. If the re-check were re-keyed onto
/// `voucher_signer`, the clean delegate would launder the blacklisted funder and
/// the 12 MiB delivery would run to completion — which is what this test fails on.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_funder_blacklisted_mid_stream_cuts_off_a_delegated_delivery()
-> Result<()> {
    // Many 1 MiB chunks, so plenty of re-check boundaries remain
    // after the deny-set flip lands. Kept under node A's 16 MiB engine cap.
    let payload = vec![0x6Bu8; 12 * 1024 * 1024];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    // The leaf funds with `leaf_funder` but signs every voucher (and its client
    // binding) with `leaf_delegate` — the split this test exists to police.
    let leaf_funder = Arc::new(PrivateKeySigner::random());
    let leaf_delegate = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x2F);
    // Starts empty: the open-time gates (`dispatch.rs`, `pull_authorized`) must
    // admit the request, so the cut-off can only come from the mid-stream check.
    let deny = Arc::new(decdn_node::content_deny::ContentDenylist::empty());
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b_with_leaves(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            &[(
                leaf_channel_id,
                leaf_funder.address(),
                leaf_delegate.address(),
                U256::from(DEPOSIT_MICRO_USDC),
            )],
            0,
            64,
            Some(Arc::clone(&deny)),
            DEFAULT_TEST_PULL_DEADLINES,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let pull_ep = leaf_ep.clone();
    let leaf_signer = Arc::clone(&leaf_delegate);
    let leaf = tokio::spawn(async move {
        leaf_paced_pull(
            &pull_ep,
            b_target,
            leaf_node_id,
            &leaf_signer,
            b_operator,
            leaf_channel_id,
            hash,
            RATE,
            None,
        )
        .await
    });

    // Wait until the window loop is demonstrably running: the pause counter only
    // ticks inside it, past the funder read at the top of the serve. Flipping the
    // deny-set before this would race the open-time gates and prove nothing.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while counter_value(&b_metrics, "node_pull_through_window_paused_total")? == 0 {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the window pull-through loop never engaged; the test never reached the \
             mid-stream re-check"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Only the FUNDER is blacklisted; the delegate signer stays clean.
    deny.apply_chain_origin(leaf_funder.address(), true);

    let outcome = tokio::time::timeout(Duration::from_secs(45), leaf)
        .await
        .map_err(|_| anyhow::anyhow!("the leaf pull never returned"))??;
    anyhow::ensure!(
        outcome.is_err(),
        "delivery completed for a blacklisted funder — the window loop's mid-stream \
         re-check has been re-keyed onto the voucher signer: {outcome:?}"
    );
    assert_counter(&b_metrics, "serve_stream_terminated_takedown_total", 1)?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// #1560 END TO END, over the wire the client actually reads: when B cannot open its
/// upstream pull because of a fault in B, the leaf must be refused `InternalError` — not the
/// signed `NotFound` that says the content does not exist.
///
/// This is the claim the issue makes, and the only test that can settle it. Everything
/// upstream of the wire can be right — the verdict classified, the counter bumped, the
/// `warn!` emitted — while the client is still told the blob is absent; that combination is
/// exactly what shipped. So the assertion is on what the leaf reads back.
///
/// B's fault is a zero `stall_timeout`, which `NodeOriginConfig::deadlines()` refuses and
/// marks `LocalPullFault` (see [`provisioned_origin_with_deadlines`]). A is healthy and
/// holds the blob throughout — that is the point. The blob exists, is reachable, and is one
/// hop away; the only thing wrong is B, and `NotFound` is therefore a false statement about
/// the content rather than a harsh-but-true one.
///
/// Both counters are asserted, in both directions. The wire code and the reject reason are
/// separate decisions (seven server-side reasons collapse onto `NotFound`), so a fix that
/// moved one without the other would leave an operator's `cache_miss` tally absorbing an
/// outage — the noisiest benign counter on the serve path hiding the loudest fault.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_local_fault_refuses_internal_error_not_not_found() -> Result<()> {
    let payload = vec![0xC1u8; 64 * 1024];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA1);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x1F);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b_with_leaves(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            &[(
                leaf_channel_id,
                leaf_eth.address(),
                leaf_eth.address(),
                U256::from(DEPOSIT_MICRO_USDC),
            )],
            0,
            64,
            None,
            // B's fault: no stall budget, so no upstream pull may legally run.
            (Duration::from_secs(20), Duration::ZERO),
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let refusal = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("B cannot pay for the upstream pull, so it cannot serve"))?;
    let refusal = refusal.to_string();
    anyhow::ensure!(
        refusal.contains("InternalError"),
        "B's own broken deadline config must be reported as B being broken, got: {refusal}"
    );
    anyhow::ensure!(
        !refusal.contains("NotFound"),
        "the blob is on A, one hop away — signing the leaf a `NotFound` is a false claim \
         about the content, which is the whole of #1560, got: {refusal}"
    );

    assert_counter(&b_metrics, "serve_stream_rejected_internal_error_total", 1)?;
    assert_counter(&b_metrics, "serve_stream_rejected_cache_miss_total", 0)?;
    // The fault is metered as ours on the pull leg too, and A — which behaved perfectly,
    // answering B's probe and then never being asked for a paid stream — keeps a clean
    // record.
    assert_counter(&b_metrics, "node_pull_local_fault_total", 1)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// The wire-level CONTROL for the test above: a healthy B whose upstream honestly lacks the
/// blob must still sign the leaf a `NotFound`.
///
/// Without this the fix is only pinned in one direction. `window.rs`'s
/// `miss_reason(fault_seen || miss.is_local_fault())` is the line #1560 changed, and
/// mutating it to a flat `miss_reason(true)` passes every other test in this file — the node
/// would answer `InternalError` for every ordinary cache miss on the network, permanently
/// steering clients off healthy nodes. That is a worse failure than the bug being fixed,
/// because it fires constantly rather than only when a node is broken.
///
/// A refuses with `NotFound` (it does not hold the blob); B's own deadlines are fine, so
/// nothing about B is at fault.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_honest_upstream_miss_still_refuses_not_found() -> Result<()> {
    let payload = vec![0xC2u8; 64 * 1024];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA2);
    let b_buyer = Arc::new(PrivateKeySigner::random());

    // A answers probes but refuses the paid stream: the honest "I don't have it" answer.
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, a_addr) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_refusing_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::NotFound,
    );

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x2F);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b_with_leaves(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            &[(
                leaf_channel_id,
                leaf_eth.address(),
                leaf_eth.address(),
                U256::from(DEPOSIT_MICRO_USDC),
            )],
            0,
            64,
            None,
            DEFAULT_TEST_PULL_DEADLINES,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let refusal = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("no upstream holds the blob, so B cannot serve it"))?;
    let refusal = refusal.to_string();
    anyhow::ensure!(
        refusal.contains("NotFound"),
        "an honest network-wide miss is exactly what a wire `NotFound` is for, got: {refusal}"
    );
    anyhow::ensure!(
        !refusal.contains("InternalError"),
        "B is healthy — reporting itself broken for an ordinary miss would steer clients \
         off a working node on every cache miss, got: {refusal}"
    );

    assert_counter(&b_metrics, "serve_stream_rejected_cache_miss_total", 1)?;
    assert_counter(&b_metrics, "serve_stream_rejected_internal_error_total", 0)?;
    assert_counter(&b_metrics, "node_pull_local_fault_total", 0)?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// #1054: an empty (0-byte) blob served via the fused window pull-through path.
/// This exercises the window pull leg's tee — `open_pull_leg` →
/// `UpstreamPull::finish` → tee promote → downstream `leaf_paced_pull` verify —
/// which the in-memory loopback e2e (`client_delivers_empty_blob`) does not
/// cover. For 0 wire bytes the window
/// never pauses, B pays A no voucher, and B still promotes the empty blob against
/// the empty root `Hash::new(&[])`.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_serves_and_caches_empty_blob() -> Result<()> {
    let payload: Vec<u8> = Vec::new();
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA1);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x1F);
    let (handler_b, b_target, ep_b, recorded, cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let outcome = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await?;

    anyhow::ensure!(outcome.completed, "leaf delivery did not complete");
    anyhow::ensure!(
        outcome.hash_ok,
        "leaf received bytes failed the empty-root check"
    );
    anyhow::ensure!(
        outcome.received == 0,
        "leaf received {} bytes, want 0",
        outcome.received
    );
    // B promotes the (verified) empty blob — it is now a discoverable holder.
    anyhow::ensure!(
        cache_b.has(hash).await?,
        "B must promote the teed empty blob"
    );
    // Zero wire bytes → B never pays A a voucher; the upstream watermark log is empty.
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "no upstream voucher for a 0-byte blob, got {:?}",
        progress_log(&recorded)?
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// #895 (#305 no-double-spend): two concurrent same-hash leaf requests against a
/// node B with an empty cache must open exactly ONE upstream pull. The first
/// request becomes the `claim_fill` Owner and pulls from A; the second is an
/// Attach observer that serves from the shared `FillSession` concurrently rather
/// than opening a second upstream pull — which would double-spend real USDC on
/// the B↔A channel. The cache-level coalescing primitive is unit-tested
/// (`engine.rs`); this pins the handler-side consequence at the layer that
/// actually spends.
///
/// Determinism: a gated upstream A parks after receiving B's (single) upstream
/// request. Because B becomes the `claim_fill` Owner before dialing upstream, the
/// gate signal proves the owner pull is in flight and B's cache is still empty,
/// so the second leaf — launched while the gate is held — is expected to Attach.
/// The no-double-spend assertions hold for every interleaving regardless.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn window_pull_through_concurrent_same_hash_single_upstream_pull() -> Result<()> {
    // A sub-interval blob keeps the gated upstream's voucher exchange to one
    // closing voucher (a multi-window cadence would need an interleaved server);
    // coalescing is independent of blob size.
    let payload = vec![0xC0u8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA8);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a, received, release) =
        spawn_gated_node_a(&payload).await?;

    // Two distinct leaf channels so the two concurrent serves do not share voucher
    // state.
    let leaf1_eth = Arc::new(PrivateKeySigner::random());
    let leaf2_eth = Arc::new(PrivateKeySigner::random());
    let leaf1_channel_id = B256::repeat_byte(0x81);
    let leaf2_channel_id = B256::repeat_byte(0x82);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b_with_leaves(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            &[
                (
                    leaf1_channel_id,
                    leaf1_eth.address(),
                    leaf1_eth.address(),
                    U256::from(DEPOSIT_MICRO_USDC),
                ),
                (
                    leaf2_channel_id,
                    leaf2_eth.address(),
                    leaf2_eth.address(),
                    U256::from(DEPOSIT_MICRO_USDC),
                ),
            ],
            0,
            64,
            None,
            DEFAULT_TEST_PULL_DEADLINES,
        )
        .await?;
    let task_b = spawn_server_concurrent(ep_b.clone(), handler_b);

    // Leaf 1: the owner pull. Spawn it, then wait for A to confirm B's single
    // upstream request landed (claim_fill Owner in flight, cache empty).
    let leaf1_sk = fresh_key();
    let leaf1_node_id = B256::from(*leaf1_sk.public().as_bytes());
    let (leaf1_ep, _) = local_endpoint(leaf1_sk, vec![]).await?;
    let leaf1_target = b_target.clone();
    let leaf1_eth_c = Arc::clone(&leaf1_eth);
    let leaf1_task = tokio::spawn(async move {
        leaf_paced_pull(
            &leaf1_ep,
            leaf1_target,
            leaf1_node_id,
            &leaf1_eth_c,
            b_operator,
            leaf1_channel_id,
            hash,
            RATE,
            None,
        )
        .await
    });

    // Bounded so a gated-server task that errored before signaling (e.g. B's
    // first upstream frame is no longer a bare `StreamRequest`) surfaces as a
    // readable failure instead of hanging until the CI job timeout.
    tokio::time::timeout(Duration::from_secs(20), received.notified())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "gated upstream A never signaled `received` — its task likely errored \
                 before reading B's upstream StreamRequest"
            )
        })?;

    // Leaf 2: the coalescing request. With the gate still held, B's cache is empty
    // and leaf 1 owns the in-flight fill as the `claim_fill` Owner, so leaf 2
    // Attaches to it. The brief pause lets leaf 2 reach that branch before we
    // release A; the no-double-spend assertions below hold regardless of
    // interleaving.
    let leaf2_sk = fresh_key();
    let leaf2_node_id = B256::from(*leaf2_sk.public().as_bytes());
    let (leaf2_ep, _) = local_endpoint(leaf2_sk, vec![]).await?;
    let leaf2_target = b_target.clone();
    let leaf2_eth_c = Arc::clone(&leaf2_eth);
    let leaf2_task = tokio::spawn(async move {
        leaf_paced_pull(
            &leaf2_ep,
            leaf2_target,
            leaf2_node_id,
            &leaf2_eth_c,
            b_operator,
            leaf2_channel_id,
            hash,
            RATE,
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Release A: bytes start flowing on the single upstream pull. Leaf 2, as the
    // Attach observer, has been running its own `serve_leg` over the shared
    // `FillSession` since it joined — it streams bytes to leaf 2 as they arrive
    // rather than waiting for the whole fill to finish — so both legs complete
    // once A finishes streaming.
    release.notify_one();

    let out1 = leaf1_task.await??;
    let out2 = leaf2_task.await??;

    for (label, out) in [("leaf 1", &out1), ("leaf 2", &out2)] {
        anyhow::ensure!(out.completed, "{label} delivery did not complete");
        anyhow::ensure!(out.hash_ok, "{label} received bytes failed the hash check");
        anyhow::ensure!(
            out.received == total_bytes,
            "{label} received {} of {total_bytes} bytes",
            out.received
        );
    }

    // The no-double-spend guarantee (#305): B persisted exactly ONE upstream
    // watermark to A, covering one blob. A second upstream pull would record a
    // second entry — the regression is caught by the entry COUNT, not the byte
    // total (a resumed second pull would re-report the same cumulative bytes, not
    // double them, since `StubOpener` resumes from the prior watermark).
    let upstream = progress_log(&recorded)?;
    anyhow::ensure!(
        upstream.len() == 1,
        "concurrent same-hash requests must open ONE upstream pull, got {upstream:?}"
    );
    let entry = upstream
        .first()
        .ok_or_else(|| anyhow::anyhow!("no upstream watermark recorded"))?;
    anyhow::ensure!(
        u64::try_from(entry.1).unwrap_or(u64::MAX) == total_bytes,
        "the single upstream pull must cover exactly one blob, got {entry:?}"
    );
    // B promoted the single coalesced fill (now a holder for future requests).
    anyhow::ensure!(
        cache_b.has(hash).await?,
        "B must promote the single coalesced fill"
    );
    // The honest upstream must not trip the corruption counter.
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_upstream_verify_failed_total")? == 0,
        "an honest upstream must not trip the verify-failed counter"
    );

    shutdown([task_a, task_b], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// The per-pool floor ceiling covers the window pull-through MISS path, not only
/// the cache-hit path: a same-lane stream already in flight makes a concurrent
/// same-lane MISS refuse — with the owner-facing `InsufficientDeposit` wire code
/// (option 2 / #2013), since the bound leaf is a proven lane owner — before it
/// ever opens a second upstream pull.
///
/// Both leaves share ONE lane — same `pool_id` (the gated upstream's channel id
/// reused as the leaf channel) and same signer, dialed over two independent
/// connections, mirroring two concurrent client streams on one payment lane.
/// A whole-blob miss reserves a full floor before it spends upstream, so
/// `remaining` covers `min_payment(floor, RATE)` with a little slack (`floor` =
/// one `CHUNK_BYTES`) — enough for the first stream's pre-spend floor reservation
/// inside `serve_via_window_pull_through` — but not `min_payment(2 * floor, RATE)`,
/// the two reservations two same-lane streams commit to the pool's floor
/// accumulator. The pool ceiling (`remaining − M`) refuses the second. This is the
/// same one-floor headroom tuning as
/// `second_same_lane_stream_refused_when_budget_covers_one` in `client_loopback.rs`,
/// extended to a request that MISSES locally and fills via the window-paced
/// pull-through provider instead of a cache hit.
///
/// Determinism: leaf 1 is admitted and its floor reservation committed at the
/// pre-spend gate — which runs before B ever dials upstream — strictly before A's
/// gated server observes the upstream `StreamRequest`. Waiting on `received`
/// therefore guarantees leaf 1's reservation is held before leaf 2 opens, so leaf 2
/// deterministically finds the pool ceiling full and is refused pre-serve (it never
/// reaches A at all).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn concurrent_same_lane_misses_refuse_surplus() -> Result<()> {
    use alloy::signers::SignerSync;

    let payload = vec![0xC7u8; 4096];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA9);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a, received, release) =
        spawn_gated_node_a(&payload).await?;

    // One lane: one leaf channel, one signer, shared by both concurrent opens.
    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x8A);
    // Exactly one floor's worth of reserved credit-window headroom, plus slack
    // strictly under a second floor. Derived from the payment quantum rather
    // than hard-coded, so it tracks `CHUNK_BYTES` instead of drifting with it.
    let remaining = decdn_incentive::min_payment(CHUNK_BYTES, RATE) + U256::from(2u64);
    let (handler_b, b_target, ep_b, _recorded, cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b_with_leaves(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            &[(
                leaf_channel_id,
                leaf_eth.address(),
                leaf_eth.address(),
                remaining,
            )],
            0,
            64,
            None,
            DEFAULT_TEST_PULL_DEADLINES,
        )
        .await?;
    let task_b = spawn_server_concurrent(ep_b.clone(), handler_b);

    // Leaf 1: opens the lane's only slot. Spawn it, then wait for A to observe
    // the upstream request — proof the hoisted gate already admitted and
    // incremented before this point.
    let leaf1_sk = fresh_key();
    let leaf1_node_id = B256::from(*leaf1_sk.public().as_bytes());
    let (leaf1_ep, _) = local_endpoint(leaf1_sk, vec![]).await?;
    let leaf1_target = b_target.clone();
    let leaf1_eth = Arc::clone(&leaf_eth);
    let leaf1_task = tokio::spawn(async move {
        leaf_paced_pull(
            &leaf1_ep,
            leaf1_target,
            leaf1_node_id,
            &leaf1_eth,
            b_operator,
            leaf_channel_id,
            hash,
            RATE,
            None,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(20), received.notified())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "gated upstream A never signaled `received` — leaf 1 likely never \
                 reached the pull-through path"
            )
        })?;

    // Leaf 2: same lane, concurrent with leaf 1 still in flight. Drive the
    // request by hand (rather than `leaf_paced_pull`, which bails on a refusal)
    // so the refusal itself — the owner-facing `InsufficientDeposit` wire code
    // (option 2 / #2013), spoken because this bound leaf is a proven lane owner —
    // is asserted.
    let leaf2_sk = fresh_key();
    let leaf2_node_id = B256::from(*leaf2_sk.public().as_bytes());
    let (leaf2_ep, _) = local_endpoint(leaf2_sk, vec![]).await?;
    let conn2 = leaf2_ep
        .connect(b_target.clone(), ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("leaf2 connect: {e}"))?;
    let (mut send2, mut recv2) = conn2
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("leaf2 open_bi: {e}"))?;
    let binding_hash = binding_signing_hash(leaf2_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = leaf_eth.sign_hash_sync(&binding_hash)?.as_bytes().to_vec();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
        capability: None,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: leaf_channel_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x9002,
    };
    let payload2 =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame(&mut send2, &payload2)
        .await
        .map_err(|e| anyhow::anyhow!("write req: {e}"))?;
    let frame2 = read_frame(&mut recv2)
        .await
        .map_err(|e| anyhow::anyhow!("read resp: {e}"))?;
    let (msg2, rest2) = decode_message::<ClientMessage>(&frame2)
        .map_err(|e| anyhow::anyhow!("decode resp: {e}"))?;
    let ClientMessage::StreamResponse(resp2) = msg2 else {
        anyhow::bail!("leaf2: expected StreamResponse, got {msg2:?}");
    };
    let resp2_ext = decdn_protocol::parse_stream_response_ext(rest2)
        .map_err(|e| anyhow::anyhow!("decode resp ext: {e}"))?;
    anyhow::ensure!(
        !resp2.body.ok,
        "concurrent same-lane MISS must be refused while budget covers only one floor"
    );
    anyhow::ensure!(
        matches!(resp2_ext.error, Some(StreamError::InsufficientDeposit)),
        "expected the owner-facing InsufficientDeposit wire code for the pool-ceiling refusal, \
         got {:?}",
        resp2_ext.error
    );
    conn2.close(0u32.into(), b"refused");

    // Release A: leaf 1, the lane's admitted stream, completes and settles —
    // proving the cap released neither wedged the lane nor blocked the admitted
    // stream.
    release.notify_one();
    let out1 = leaf1_task.await??;
    anyhow::ensure!(out1.completed, "leaf 1 delivery did not complete");
    anyhow::ensure!(out1.hash_ok, "leaf 1 received bytes failed the hash check");
    anyhow::ensure!(
        cache_b.has(hash).await?,
        "leaf 1's fill must promote the blob"
    );

    shutdown([task_a, task_b], [&leaf2_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A BOUNDED, UNALIGNED range (`byte_offset` and end both inside chunk groups)
/// through the peer fused path: leaf→B (cold miss)→A. The serve leg's paid-wire
/// mapping starts at the group floor of the offset and the pull leg draws only
/// the aligned span's missing groups — proven by B's recorded upstream spend
/// being exactly the aligned span's bao wire, and B's cache holding the span
/// but not the head. Unlike the own-origin twin, this leg fronts real upstream
/// payment, so an off-by-one-group here over- or under-draws against a live
/// counterparty.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn window_pull_through_bounded_unaligned_range_pulls_only_the_span() -> Result<()> {
    let payload: Vec<u8> = (0..PAYLOAD_LEN)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let hash = Hash::new(&payload);
    let total = u64::try_from(PAYLOAD_LEN)?;

    let ab_channel_id = B256::repeat_byte(0xA9);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x8F);
    let (handler_b, b_target, ep_b, recorded, cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;

    // 20 KiB is NOT a 16 KiB group boundary; the 40 KiB length ends mid-group
    // too. The aligned superset is [16 KiB, 64 KiB).
    let (req_off, req_len) = (20 * 1024u64, 40 * 1024u64);
    let aligned = decdn_cache::range_pull::align_range(req_off, req_len, total)
        .map_err(|e| anyhow::anyhow!("align: {e}"))?;

    let got = leaf_ranged_paid_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        req_off,
        req_len,
        RATE,
    )
    .await?;
    let want = payload
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "unaligned bounded range through the peer path must be byte-exact"
    );

    // B's upstream spend covered exactly the aligned span's bao wire — the
    // range-minimized pull, priced against a live counterparty.
    let span_wire = decdn_cache::range_pull::bao_encoded_size(total, aligned.chunk_ranges());
    let log = progress_log(&recorded)?;
    let last = log
        .last()
        .ok_or_else(|| anyhow::anyhow!("B must have pulled upstream"))?;
    anyhow::ensure!(
        last.1 == U256::from(span_wire),
        "B's upstream wire must be the aligned span's bao size ({span_wire}), got {}",
        last.1
    );
    // The span is present in B's cache; the head was never pulled.
    anyhow::ensure!(!cache_b.has(hash).await?, "B holds a partial, not the blob");
    anyhow::ensure!(
        cache_b
            .missing_ranges(hash, req_off, req_len, total)
            .await?
            .is_empty(),
        "the requested span must be present in B's cache"
    );
    anyhow::ensure!(
        !cache_b
            .missing_ranges(hash, 0, aligned.fetch_start(), total)
            .await?
            .is_empty(),
        "the head before the aligned span must never have been pulled"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A resumed cache-miss request (`byte_offset > 0`) IS served by the fused window
/// path: the serve leg clamps delivery to `[offset, end)`, the pull leg fills only
/// `missing_ranges(offset, 0)`, and every chunk group verifies against the root
/// independently (ADR 038), so no tier needs byte 0. The proof is the drained,
/// byte-exact tail — a fused path serving from byte 0, or a buffered fallback on
/// B's empty cache (which would refuse), both fail it.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn window_pull_through_resumed_offset_is_served_by_the_fused_path() -> Result<()> {
    let payload = vec![0x7Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x7F);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;

    // Resume from one interval in (byte_offset > 0) and DRAIN the delivery: the
    // routing proof is the byte-exact tail, not just the signed `ok: true` — a
    // fused path that served from byte 0 would fail the comparison.
    let got = leaf_ranged_paid_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        MB_BYTES,
        0,
        RATE,
    )
    .await?;
    let want = payload
        .get(usize::try_from(MB_BYTES)?..)
        .ok_or_else(|| anyhow::anyhow!("tail out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "the fused path must serve the requested tail byte-exact"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_drop_after_fill_bounds_upstream_spend() -> Result<()> {
    // The #856 attack: a leaf that owns a channel requests a large blob, takes
    // the first interval, acks one voucher, then drops. B must stop pulling — its
    // upstream spend is bounded to ~one window, NOT the whole blob.
    let payload = vec![0x4Du8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA2);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x2F);
    let (handler_b, b_target, ep_b, recorded, _cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let outcome = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        Some(1),
    )
    .await?;
    anyhow::ensure!(
        !outcome.completed,
        "leaf was supposed to drop, not complete"
    );
    anyhow::ensure!(
        outcome.acks == 1,
        "leaf should have acked exactly one voucher"
    );

    // Give B a moment to observe the drop and persist its bounded watermark.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The headline #856 bound under the DECOUPLED pull leg (#1621). The leg paces
    // on the CONTENT frontier (`WindowPacer`, ADR 037) and keeps at most one window
    // of UNPAID content in flight, so once the leaf's single paid interval sits
    // within a window of the blob's end the leg finishes the whole sub-2-window
    // blob. The invariant is therefore on the UNRECOUPED lead, not the total
    // pulled: `upstream_bytes - paid <= window + group`.
    let log = progress_log(&recorded)?;
    let upstream_bytes: u64 = log
        .last()
        .map_or(0, |(_, bytes, _)| u64::try_from(*bytes).unwrap_or(u64::MAX));
    // What the leaf actually paid for: it acked exactly one 1 MiB chunk
    // before dropping (asserted above), so the content it received is the concrete
    // stand-in for its paid frontier (~one window).
    let paid = outcome.received;
    // The ramped credit window (#1669): at `paid = 0` it floors to one voucher
    // interval, 1 MiB here.
    let one_window = MB_BYTES;
    // `upstream_bytes` is the bao WIRE (content + interleaved proof, ADR 038); one
    // chunk group of slack absorbs the boundary chunk group plus the proof overhead
    // over the window.
    let group = decdn_cache::CHUNK_GROUP_BYTES;
    anyhow::ensure!(
        upstream_bytes.saturating_sub(paid) <= one_window + group,
        "B's UNRECOUPED upstream lead ({upstream_bytes} pulled - {paid} paid) must be bounded to \
         ~one window ({one_window}), not run open-ended against the {total_bytes}-byte blob"
    );
    // Caching a fully-pulled sub-2-window blob is ALLOWED: the #856 spend bound
    // holds as bounded UNRECOUPED lead (above), not as total-pulled, so a leaf that
    // paid one interval of a 1.5-window blob may still leave B holding the finished
    // fill. No promotion assertion either way — this test polices spend, not caching.

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn window_pull_through_connected_nonpaying_leaf_past_ramp_bounds_upstream_spend() -> Result<()>
{
    // A leaf pays three vouchers, so both of B's windows ramp past their floors,
    // then stays connected and keeps reading without paying. B's unrecouped
    // upstream lead must stay within the ramped credit window plus one
    // serve-demand pull-window floor (ADR 037), however large the blob is. This
    // bounds the lead from above; `serve_demand_at_a_full_window_draws_one_floor`
    // in the pacer pins the exact demand draw.
    let payload_len = 8 * CHUNK_BYTES;
    let payload = vec![0x5Au8; usize::try_from(payload_len)?];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA9);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x9A);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    // Three paid intervals put `paid / DEFAULT_CREDIT_RAMP_DIVISOR` above
    // `PULL_WINDOW_FLOOR`, so the pull window has ramped. The 2 s hold is shorter
    // than B's 10 s `VOUCHER_READ_TIMEOUT`, so B does not end the stream while
    // the leaf holds it open.
    let outcome = tokio::time::timeout(
        Duration::from_mins(1),
        leaf_paced_pull_mode(
            &leaf_ep,
            b_target,
            leaf_node_id,
            &leaf_eth,
            b_operator,
            leaf_channel_id,
            hash,
            RATE,
            LeafMode::StopPayingAfter {
                acks: 3,
                hold: Duration::from_secs(2),
            },
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("leaf pull did not finish within a minute"))??;
    anyhow::ensure!(
        !outcome.completed,
        "leaf stopped paying, so B must not deliver the whole blob"
    );
    anyhow::ensure!(
        outcome.acks == 3,
        "leaf should have paid exactly three vouchers, got {}",
        outcome.acks
    );
    // The hold reached saturation: B served its whole ramped serve window past
    // the last voucher, and its pull leg paused on a closed window. Without
    // these, a B that stalled early would pass every upper bound below.
    let paid = outcome.paid_wire;
    let group = decdn_cache::CHUNK_GROUP_BYTES;
    let serve_window = decdn_incentive::ramped_credit_window(
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
        CHUNK_BYTES,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        paid,
    );
    anyhow::ensure!(
        outcome.received + group >= paid + serve_window,
        "B must fill its serve credit window ({serve_window}) past the last voucher \
         during the hold ({} received, {paid} paid)",
        outcome.received
    );
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_window_paused_total")? >= 1,
        "B's pull leg must have paused on its window"
    );

    // B records its upstream watermark when the pull leg ends, which the leaf's
    // close triggers.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let upstream_wire = loop {
        if let Some(&(_, bytes, _)) = progress_log(&recorded)?.last() {
            break u64::try_from(bytes)
                .map_err(|_| anyhow::anyhow!("upstream watermark {bytes} overflows u64"))?;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "B never recorded an upstream watermark: the pull leg did not settle \
             (persist failures: {})",
            counter_value(&b_metrics, "node_pull_progress_persist_failures_total")?
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // Content never exceeds its wire, and the ramp only widens with payment, so
    // taking the paid wire as the paid content over-states the window.
    let pull_floor = decdn_client::PULL_WINDOW_FLOOR;
    let ramped = decdn_incentive::ramped_credit_window(
        decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
        pull_floor,
        decdn_common::config::DEFAULT_CREDIT_MAX,
        paid,
    );
    anyhow::ensure!(
        ramped > pull_floor,
        "the pull window must have ramped past its floor ({ramped} <= {pull_floor}), \
         or this test does not cover the ramp"
    );
    // ADR 037 bounds the unrecouped frontier as bytes pulled minus bytes paid.
    // Measuring against the paid wire, not the larger wire the leaf received
    // inside B's serve credit window, keeps the bound tight.
    //
    // The upstream watermark sums the wire B paid across every pull-leg open, and
    // each open re-sends the proof path to its start, which one range encoding
    // counts once. A single-group range carries a full root-to-leaf path, so its
    // wire minus its content is the most one extra clean open adds. B opens at
    // most once per served-paid advance and once per serve-demand floor, so an
    // allowance of 32 opens is generous. The test does not count opens, and the
    // allowance stays far below the one floor a breach would add.
    let proof_path = support::bao_wire_len(payload_len, 0, group).saturating_sub(group);
    let max_opens = 32;
    let bound_wire =
        support::bao_wire_len(payload_len, 0, ramped + pull_floor) + max_opens * proof_path;
    let lead = upstream_wire.saturating_sub(paid);
    anyhow::ensure!(
        lead <= bound_wire,
        "B's unrecouped upstream lead ({upstream_wire} upstream - {paid} paid = {lead}) must \
         stay within the ramped window ({ramped}) plus one pull-window floor ({pull_floor}) \
         plus {max_opens} proof paths of {proof_path} = {bound_wire} wire bytes; leaf \
         received {}",
        outcome.received
    );
    let whole_wire = support::bao_wire_len_whole(payload_len);
    anyhow::ensure!(
        upstream_wire < whole_wire,
        "a leaf that stopped paying must not make B pull the whole blob \
         ({upstream_wire} >= {whole_wire})"
    );
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "B must not cache a blob it never finished pulling"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_insufficient_deposit_refuses_before_pulling() -> Result<()> {
    // Pre-flight floor-`M` deposit guard (shared-payment-pool model): a pool whose
    // on-chain `remaining` (here the stub pool-view reports the seeded deposit)
    // minus the refundable floor `M` can no longer cover the reserved credit
    // window is refused (signed `NotFound`) BEFORE any upstream pull — no USDC
    // fronted. The reserved window is one voucher accounting interval at `RATE`;
    // a remaining of 5 cannot cover it and the pull-through is refused at the
    // pre-spend gate.
    let payload = vec![0x9Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA3);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x3F);
    let max_blob_size_bytes = 64 * 1024 * 1024;
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(5u64),
            max_blob_size_bytes,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let refused = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused,
        "an underfunded channel must be refused (signed NotFound), not served"
    );
    // No upstream pull was attempted: nothing persisted, nothing cached.
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "the deposit guard must reject before any upstream spend, got {:?}",
        progress_log(&recorded)?
    );
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "a deposit-refused request must not fill B's cache"
    );
    assert_counter(
        &b_metrics,
        "serve_stream_rejected_insufficient_deposit_total",
        1,
    )?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// Spin up a lying upstream A: it answers probes truthfully (`has_blob`) but
/// serves wrong bytes of the advertised length on the client stream. Same shape
/// as [`spawn_node_a`] so it drops into [`build_node_b`].
async fn spawn_lying_node_a(
    served_wrong: Vec<u8>,
    advertised_bytes: u64,
) -> Result<(
    iroh::PublicKey,
    std::net::SocketAddr,
    Arc<PrivateKeySigner>,
    iroh::Endpoint,
    tokio::task::JoinHandle<()>,
)> {
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_lying_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        served_wrong,
        advertised_bytes,
        RATE,
    );
    Ok((a_id, addr_a, a_eth, ep_a, task_a))
}

/// One scripted upstream's view of a lane's claim under `PayWord` (ADR 003
/// §Hash-chain metering).
///
/// A proof is a signed voucher or a released preimage. The voucher sets the
/// anchor and the chunk price; each reveal extends that anchor by
/// `index × chunk_price` without a signature. These stubs model
/// `PaymentPool.redeemMany`'s arithmetic, so they resolve the same way it does:
/// `claimed = amount + chain_index × chunk_price`.
#[derive(Debug, Default, Clone, Copy)]
struct ScriptedLane {
    anchor: U256,
    price: U256,
    index: u8,
    /// The root the lane meters against. Tracked because it is what decides
    /// whether an incoming voucher retires the frontier or merely re-states it.
    root: B256,
}

impl ScriptedLane {
    /// What the lane is owed right now.
    fn claim(self) -> U256 {
        self.anchor
            .saturating_add(self.price.saturating_mul(U256::from(self.index)))
    }

    /// Fold one proof in, reporting whether it ADVANCED the claim.
    ///
    /// A voucher that merely re-asserts the live root advances nothing — that is
    /// the per-stream re-anchor every stream sends before its first reveal of an
    /// epoch, and a real node treats it as already-satisfied.
    ///
    /// Which is precisely why the frontier turns on the ROOT, not on the mere
    /// arrival of a voucher. Only a voucher naming a DIFFERENT root retires the
    /// live chain, and its amount has folded that chain's frontier in as the price
    /// of doing so; one re-stating the live root leaves the frontier where it was.
    /// Resetting the index on every voucher would let a re-anchor erase chunks the
    /// payer has already revealed and paid for, collapsing the lane's claim back
    /// to its anchor — see `LaneState::advance_presigned`, whose rule this mirrors.
    fn apply(&mut self, proof: &ClientMessage) -> bool {
        let before = self.claim();
        match proof {
            ClientMessage::Voucher(v) => {
                let root = B256::from(v.chain_root);
                if root != self.root {
                    self.root = root;
                    self.index = 0;
                }
                // An already-satisfied voucher does not move the node's watermark,
                // so take the high-water mark rather than whatever this one carries.
                self.anchor = self.anchor.max(U256::from(v.amount));
                self.price = U256::from(v.chunk_price);
            }
            ClientMessage::ChunkPreimage(p) => {
                self.index = self.index.max(p.index);
            }
            _ => return false,
        }
        self.claim() > before
    }
}

/// Read one payment proof — a `Voucher` or a `ChunkPreimage`, the whole
/// payer→node vocabulary after the opening request.
async fn read_proof(recv: &mut iroh::endpoint::RecvStream) -> Result<ClientMessage> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("read proof: {e}"))?;
    let (msg, _) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    match msg {
        ClientMessage::Voucher(_) | ClientMessage::ChunkPreimage(_) => Ok(msg),
        other => anyhow::bail!("scripted upstream: expected a payment proof, got {other:?}"),
    }
}

/// Read one payment proof — the per-chunk exchange [`serve_wire_paced`] performs
/// at each 1 MiB boundary. Acceptance is implicit (continued delivery is the
/// ack, ADR 005), so no reply is written.
async fn read_voucher(recv: &mut iroh::endpoint::RecvStream) -> Result<()> {
    read_proof(recv).await.map(|_| ())
}

/// Like [`serve_wrong_bytes`], but serves `wire` (a bao verified-stream,
/// possibly corrupted mid-way) with the REAL per-interval voucher pacing:
/// `total_bytes` (the CONTENT size) is advertised separately from the wire
/// length, and a voucher is read + acked at every chunk boundary of
/// wire bytes, matching the buyer's cadence — so a multi-interval serve never
/// deadlocks on an unacked mid-stream voucher. When the buyer aborts (e.g. its
/// tee rejects a corrupt group, #915), the next write/read here errors and the
/// spawner ignores it.
///
/// `gap` sleeps between consecutive frames (`Duration::ZERO` ⇒ as fast as the
/// wire allows, the historical behaviour). It is what lets a caller stretch a
/// perfectly HEALTHY transfer past the per-candidate `pull_timeout` without any
/// single inter-frame gap looking like a stall (#1134) — this fixture's voucher
/// pacing is per-interval, not per-second, so before `gap` nothing in the suite
/// could move a transfer past a deadline at all.
#[allow(clippy::too_many_arguments)]
async fn serve_wire_paced(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    wire: &[u8],
    total_bytes: u64,
    rate: u64,
    gap: Duration,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read request: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode request: {e}"))?
            .0
    };
    let ClientMessage::StreamRequest(req) = req_msg else {
        anyhow::bail!("paced upstream: expected a StreamRequest");
    };
    let body = StreamResponseBody {
        hash: req.hash,
        ok: true,
        rate_per_mb: rate,
        total_bytes,
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
    let interval_bytes = CHUNK_BYTES;
    let mut unvouchered: u64 = 0;
    for chunk in wire.chunks(WIRE_FRAME) {
        if !gap.is_zero() {
            tokio::time::sleep(gap).await;
        }
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
        unvouchered = unvouchered.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if unvouchered >= interval_bytes {
            read_voucher(&mut recv).await?;
            unvouchered = 0;
        }
    }
    if unvouchered > 0 {
        read_voucher(&mut recv).await?;
    }
    write_frame(&mut send, &encode_message(&ClientMessage::StreamEnd)?)
        .await
        .map_err(|e| anyhow::anyhow!("write end: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// [`spawn_lying_node_a`] variant whose client-path serve is the interval-paced
/// [`serve_wire_paced`] (bao wire + separate advertised content size), for
/// multi-interval corrupt-serve tests (#915).
async fn spawn_paced_lying_node_a(
    wire: Vec<u8>,
    total_bytes: u64,
) -> Result<(
    iroh::PublicKey,
    std::net::SocketAddr,
    Arc<PrivateKeySigner>,
    iroh::Endpoint,
    tokio::task::JoinHandle<()>,
)> {
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let slash = slash_domain();
    let eth = Arc::clone(&a_eth);
    let accept_ep = ep_a.clone();
    let task_a = tokio::spawn(async move {
        while let Some(incoming) = accept_ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&eth);
            let dom = slash.clone();
            let wire = wire.clone();
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, RATE, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_wire_paced(
                        conn,
                        &eth,
                        &dom,
                        &wire,
                        total_bytes,
                        RATE,
                        Duration::ZERO,
                    )
                    .await;
                });
            }
        }
    });
    Ok((a_id, addr_a, a_eth, ep_a, task_a))
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_lying_upstream_is_not_cached() -> Result<()> {
    // #856/#915: a bait-and-switch upstream serves a wire-COMPLETE stream whose
    // bytes fail bao verification against the requested root. `pull.finish(..)`
    // is Ok (the promised wire byte count arrived) — the corruption is caught by
    // the TEE's bao decoder and surfaces at `tee.finish()`. B must NOT promote
    // the corrupt blob, the leaf's own bao decoder rejects the forward, the
    // `upstream_verify_failed` counter fires (and the local-fault counters do
    // NOT — the arms are mutually exclusive), and A is scored `Corruption` (not
    // `Delivered`) in B's local reputation. A small (single-interval,
    // single-leaf: wire == content for ≤16 KiB) blob keeps the buyer↔upstream
    // voucher exchange to one closing voucher.
    let honest = vec![0x77u8; 4096];
    let hash = Hash::new(&honest);
    let served_wrong = vec![0x88u8; 4096];
    anyhow::ensure!(Hash::new(&served_wrong) != hash, "fixtures must differ");
    let advertised_bytes = u64::try_from(honest.len()).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA6);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_lying_node_a(served_wrong, advertised_bytes).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x6F);
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);
    // A's reputation before the pull: the corrupt serve must LOWER it (#915 —
    // pre-fix, a wire-complete corrupt upstream banked a `Delivered` and the
    // score went UP).
    let a_score_before = local_rep.score(a_id);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    // The serve forwards corrupt bytes, then closes without a `StreamEnd` once the
    // upstream fails verification, so the leaf's read returns an error rather than
    // a clean completion — either way it must not see a verified blob.
    let outcome = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await;
    if let Ok(o) = &outcome {
        anyhow::ensure!(
            !o.hash_ok,
            "a corrupt-upstream serve must never deliver a hash-verified blob"
        );
    }

    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "B must not promote a blob that failed upstream verification"
    );
    assert_counter(
        &b_metrics,
        "node_pull_through_upstream_verify_failed_total",
        1,
    )?;
    // The verify verdict reached the scorer: A recorded a `Corruption` observation,
    // so its local score dropped below the pre-pull baseline (#915).
    let a_score_after = local_rep.score(a_id);
    anyhow::ensure!(
        a_score_after < a_score_before,
        "a wire-complete corrupt upstream must be scored Corruption (score {a_score_before} -> {a_score_after})"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_mid_stream_corruption_scores_upstream_not_local() -> Result<()> {
    // #915 review: a corrupt group MID-stream — past the first chunk,
    // with plenty of wire still to come — kills the verifying decoder while the
    // node is still forwarding, so the failure surfaces mid-stream rather than at
    // the end. This is the dominant real-world corruption shape. It must be
    // classified as an UPSTREAM fault, not a local store fault: abandon the pull
    // early (bounded spend), not promote, fire `upstream_verify_failed`, and score
    // A `Corruption`.
    //
    // Construction: the HONEST whole-blob bao wire for a payload 1.5x one voucher
    // accounting interval (so one voucher exchange completes before the
    // corruption), with a single byte flipped just past that first interval —
    // every group before it verifies, the containing group fails, and a real
    // remainder of wire stays undelivered behind it.
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(3) / 2).unwrap_or(usize::MAX);
    let payload = vec![0xB7u8; payload_len];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        &payload,
        decdn_cache::range_pull::IROH_BLOCK_SIZE,
    );
    let aligned = decdn_cache::range_pull::align_range(0, 0, total_bytes)?;
    let combined = decdn_cache::range_pull::encode_verified_range(
        *hash.as_bytes(),
        &aligned,
        &payload,
        bytes::Bytes::from(ob.data),
    )?;
    // Strip the 8-byte LE size header (the wire is header-less) and corrupt one
    // byte past the first chunk.
    let mut wire = combined
        .get(8..)
        .ok_or_else(|| anyhow::anyhow!("combined encoding shorter than its header"))?
        .to_vec();
    let corrupt_at = usize::try_from(CHUNK_BYTES.saturating_add(400_000)).unwrap_or(usize::MAX);
    let byte = wire
        .get_mut(corrupt_at)
        .ok_or_else(|| anyhow::anyhow!("corruption offset outside the wire"))?;
    *byte ^= 0xFF;

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) = spawn_paced_lying_node_a(wire, total_bytes).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x7A);
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);
    let a_score_before = local_rep.score(a_id);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    // B aborts mid-forward once the tee rejects, so the leaf sees a reset (or a
    // truncated stream at best) — never a verified blob.
    let outcome = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await;
    if let Ok(o) = &outcome {
        anyhow::ensure!(
            !o.hash_ok,
            "a mid-stream-corrupt serve must never deliver a verified blob"
        );
    }

    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "B must not promote a blob whose tee rejected a group mid-stream"
    );
    assert_counter(
        &b_metrics,
        "node_pull_through_upstream_verify_failed_total",
        1,
    )?;
    let a_score_after = local_rep.score(a_id);
    anyhow::ensure!(
        a_score_after < a_score_before,
        "mid-stream corruption must be scored Corruption (score {a_score_before} -> {a_score_after})"
    );

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A leaf's paid stream to B, opened and read up to the first interval
/// boundary, where B parks for the first voucher.
struct LeafStream {
    /// The leaf's connection to B, which the stream lives on.
    conn: iroh::endpoint::Connection,
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
    /// Wire bytes the leaf has read.
    delivered: u64,
}

/// Open a paid stream from the leaf to B for `hash` and read chunks until the
/// first interval boundary.
async fn leaf_reads_first_interval(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    pool_id: B256,
    hash: Hash,
) -> Result<LeafStream> {
    let conn = leaf_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("leaf connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("leaf open_bi: {e}"))?;

    let binding_hash = binding_signing_hash(leaf_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = {
        use alloy::signers::SignerSync;
        leaf_eth.sign_hash_sync(&binding_hash)?.as_bytes().to_vec()
    };
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
        capability: None,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        pool_id: pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x9002,
    };
    write_frame(
        &mut send,
        &encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write req: {e}"))?;

    let (resp, resp_ext) = read_client_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error);
    let interval_bytes = CHUNK_BYTES;

    let mut delivered: u64 = 0;
    loop {
        match read_client(&mut recv).await? {
            ClientMessage::ChunkData(chunk) => {
                delivered = delivered.saturating_add(chunk.bytes().len() as u64);
                if delivered >= interval_bytes {
                    break;
                }
            }
            ClientMessage::StreamEnd => anyhow::bail!("stream ended before the first interval"),
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }
    Ok(LeafStream {
        conn,
        send,
        recv,
        delivered,
    })
}

/// Sign a sealed voucher from the leaf for `(bytes_delivered, amount)` and write
/// it to B.
async fn leaf_sends_voucher(
    send: &mut iroh::endpoint::SendStream,
    leaf_eth: &Arc<PrivateKeySigner>,
    provider: Address,
    pool_id: B256,
    bytes_delivered: u64,
    amount: U256,
) -> Result<()> {
    let signed = Voucher {
        pool_id,
        signer: leaf_eth.address(),
        provider,
        amount,
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(leaf_eth.as_ref(), &voucher_dom())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client(
        send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)?),
    )
    .await
}

/// A leaf that takes the first interval of bytes, then signs and sends a voucher
/// that *underpays* (a token amount well below the quoted rate) instead of paying.
/// B answers with an `Underpaid` rejection mid-window. Returns once it observes
/// B's `StreamError` rejection (or the stream drops).
async fn leaf_underpays_first_voucher(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    provider: Address,
    pool_id: B256,
    hash: Hash,
) -> Result<()> {
    let mut leaf =
        leaf_reads_first_interval(leaf_ep, target, leaf_node_id, leaf_eth, pool_id, hash).await?;
    // Far below the quoted rate for one interval.
    leaf_sends_voucher(
        &mut leaf.send,
        leaf_eth,
        provider,
        pool_id,
        leaf.delivered,
        U256::from(1u64),
    )
    .await?;

    // B must reject the underpayment in band, as `Underpaid`: a dropped stream
    // would leave the payer no reason to act on.
    match read_client(&mut leaf.recv).await? {
        ClientMessage::StreamError(StreamError::VoucherRejected {
            reason: VoucherRejectReason::Underpaid,
            ..
        }) => Ok(()),
        other => anyhow::bail!("expected an Underpaid rejection, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_underpaid_voucher_abandons_bounded() -> Result<()> {
    // #856: a leaf that underpays a mid-window voucher must be cleanly rejected
    // (`Underpaid`), B must abandon the partial fill (nothing cached),
    // its upstream spend stays bounded to ~one window, and the client-abandoned
    // counter fires. The payload exceeds one voucher accounting interval
    // (`CHUNK_BYTES`), so the leaf actually reaches an interval boundary
    // to underpay rather than draining the whole blob first.
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(3) / 2).unwrap_or(usize::MAX);
    let payload = vec![0x7Cu8; payload_len];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x7F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    leaf_underpays_first_voucher(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
    )
    .await?;

    // Wait for B's pull leg to SETTLE, not for a fixed span. The settle is what
    // writes the upstream watermark this test reads, so a sleep that returns first
    // leaves the log empty — and an empty log reads as a spend of zero, which passes
    // the bound below while proving nothing.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    // Exactly one settle per leg, so the first non-empty read is the final
    // watermark, not a partial one.
    let settled = loop {
        if let Some(&(_, bytes, _)) = progress_log(&recorded)?.last() {
            break bytes;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "B never recorded an upstream watermark: the pull leg did not settle"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "an underpaid serve must not promote the partial blob"
    );
    let upstream_bytes: u64 = u64::try_from(settled).unwrap_or(u64::MAX);
    // The ramped credit window (#1669) at `paid = 0` is the pacing floor, which is
    // `credit_floor.max(PULL_WINDOW_FLOOR)`. `PULL_WINDOW_FLOOR` carries two chunk
    // groups on top of one voucher interval so the two group-sized roundings between
    // paid wire and drawable content cannot park the pull short of the chunk the
    // client must complete to pay — so the floor is NOT one bare interval.
    let one_window = decdn_client::PULL_WINDOW_FLOOR;
    // The window bounds CONTENT bytes; the upstream watermark meters WIRE bytes (bao
    // content plus interleaved proof, ADR 038). Convert rather than adding slack: the
    // bound is then exact, and a pull that draws one group past the window fails here
    // instead of hiding inside a tolerance.
    let window_wire = support::bao_wire_len(
        u64::try_from(payload_len).unwrap_or(u64::MAX),
        0,
        one_window,
    );
    anyhow::ensure!(
        upstream_bytes <= window_wire,
        "B's upstream spend ({upstream_bytes} wire) must stay bounded to one credit \
         window ({one_window} content = {window_wire} wire)"
    );
    assert_counter(&b_metrics, "node_pull_through_client_abandoned_total", 1)?;
    assert_counter(&b_metrics, "serve_stream_proof_budget_exhausted_total", 0)?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A proof that pays part of an outstanding chunk leaves the rest owed on the
/// miss path too (#2132).
///
/// The leaf pays the first interval by a sliver only. B must keep the rest of
/// that interval owed and deliver nothing more until a proof pays it. If B drops
/// the remainder, the sliver reopens the credit window for bytes nobody paid for.
/// Once the leaf pays the rest, the stream must run to a clean, fully paid end.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_sliver_voucher_leaves_the_rest_owed() -> Result<()> {
    const SLIVER_BYTES: u64 = 64 * 1024;
    // Three intervals, so the stream is still mid-blob when the first one is paid.
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(3)).unwrap_or(usize::MAX);
    let payload = vec![0x2Du8; payload_len];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA8);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x2E);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, _b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let mut leaf = leaf_reads_first_interval(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        leaf_channel_id,
        hash,
    )
    .await?;

    // Pay the first interval by a sliver. The voucher pays the quoted rate for
    // the span it claims, so B accepts it and credits exactly the sliver.
    let mut amount = min_payment(SLIVER_BYTES, RATE);
    leaf_sends_voucher(
        &mut leaf.send,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        SLIVER_BYTES,
        amount,
    )
    .await?;

    // The rest of the interval is still owed, so B delivers nothing more.
    match tokio::time::timeout(Duration::from_secs(2), read_client(&mut leaf.recv)).await {
        Err(_elapsed) => {}
        Ok(Ok(ClientMessage::ChunkData(chunk))) => anyhow::bail!(
            "B sent {} more wire bytes on a sliver payment instead of waiting for the rest",
            chunk.bytes().len()
        ),
        Ok(other) => anyhow::bail!("expected B to wait for the rest of the payment, got {other:?}"),
    }

    // From here on the leaf pays for every byte it has read whenever B goes
    // quiet. The first voucher settles the rest of the first interval, and the
    // stream must run to a clean `StreamEnd`, fully paid. A quiet spell with
    // every byte paid is B pulling from A, so the leaf just waits, within an
    // overall deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut paid = SLIVER_BYTES;
    loop {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "B never finished the stream ({} bytes read, {paid} paid)",
            leaf.delivered
        );
        match tokio::time::timeout(Duration::from_millis(500), read_client(&mut leaf.recv)).await {
            Ok(Ok(ClientMessage::ChunkData(chunk))) => {
                leaf.delivered = leaf.delivered.saturating_add(chunk.bytes().len() as u64);
            }
            Ok(Ok(ClientMessage::StreamEnd)) => break,
            Err(_quiet) if leaf.delivered == paid => {}
            Err(_quiet) => {
                amount += min_payment(leaf.delivered - paid, RATE);
                paid = leaf.delivered;
                leaf_sends_voucher(
                    &mut leaf.send,
                    &leaf_eth,
                    b_operator,
                    leaf_channel_id,
                    paid,
                    amount,
                )
                .await?;
            }
            Ok(other) => anyhow::bail!("expected the stream to run to StreamEnd, got {other:?}"),
        }
    }
    anyhow::ensure!(
        paid == leaf.delivered,
        "B ended the stream with {} unpaid bytes",
        leaf.delivered - paid
    );

    leaf.conn.close(0u32.into(), b"done");
    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A leaf that answers one chunk with sliver after sliver hits the per-chunk
/// proof budget on the miss path too (#2132). Each sliver credits something, but
/// none settles the chunk, so B ends the stream and meters both the client
/// abandon and the spent proof budget instead of holding the stream and its
/// upstream pull open one sliver at a time.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_sliver_vouchers_exhaust_the_proof_budget() -> Result<()> {
    const SLIVER_BYTES: u64 = 64 * 1024;
    // The node's private `MAX_PROOFS_PER_CHUNK`; keep the two in step.
    const PROOF_BUDGET: u64 = 8;
    anyhow::ensure!(
        PROOF_BUDGET * SLIVER_BYTES < CHUNK_BYTES,
        "the slivers must not add up to a whole interval"
    );
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(3)).unwrap_or(usize::MAX);
    let payload = vec![0x3Eu8; payload_len];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA9);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x3F);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let mut leaf = leaf_reads_first_interval(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        leaf_channel_id,
        hash,
    )
    .await?;

    let mut amount = U256::ZERO;
    for n in 1..=PROOF_BUDGET {
        amount += min_payment(SLIVER_BYTES, RATE);
        leaf_sends_voucher(
            &mut leaf.send,
            &leaf_eth,
            b_operator,
            leaf_channel_id,
            n * SLIVER_BYTES,
            amount,
        )
        .await?;
    }

    // The stream ends with a payment fault: no further byte and no `StreamEnd`.
    match tokio::time::timeout(Duration::from_secs(10), read_client(&mut leaf.recv)).await {
        Err(_elapsed) => anyhow::bail!("B kept the stream open past the proof budget"),
        Ok(Ok(ClientMessage::ChunkData(chunk))) => anyhow::bail!(
            "B delivered {} more wire bytes on a chunk that is still owed",
            chunk.bytes().len()
        ),
        Ok(Ok(other)) => anyhow::bail!("expected the stream to fail, got {other:?}"),
        Ok(Err(_)) => {}
    }
    assert_counter(&b_metrics, "node_pull_through_client_abandoned_total", 1)?;
    assert_counter(&b_metrics, "serve_stream_proof_budget_exhausted_total", 1)?;

    leaf.conn.close(0u32.into(), b"done");
    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A lane-store fault while B recoups a proof on the miss path is B's own fault,
/// not a client abandon (#2134). The leaf pays a valid voucher, B's `record`
/// fails, and the stream ends. The dispatch sink meters the fault as a node
/// fault; the client-abandon counter stays at zero.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_store_record_failure_is_a_node_fault_not_an_abandon() -> Result<()> {
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(3)).unwrap_or(usize::MAX);
    let payload = vec![0x4Cu8; payload_len];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xAA);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x4D);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b_with_store(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            &[(
                leaf_channel_id,
                leaf_eth.address(),
                leaf_eth.address(),
                U256::from(DEPOSIT_MICRO_USDC),
            )],
            0,
            64,
            None,
            DEFAULT_TEST_PULL_DEADLINES,
            |store| {
                Arc::new(support::FailingRecordStore { inner: store }) as Arc<dyn PoolStateStore>
            },
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let mut leaf = leaf_reads_first_interval(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        leaf_channel_id,
        hash,
    )
    .await?;
    leaf_sends_voucher(
        &mut leaf.send,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        CHUNK_BYTES,
        min_payment(CHUNK_BYTES, RATE),
    )
    .await?;

    // The stream ends on the fault: no further byte and no `StreamEnd`.
    match tokio::time::timeout(Duration::from_secs(10), read_client(&mut leaf.recv)).await {
        Err(_elapsed) => anyhow::bail!("B kept the stream open past a lane-store fault"),
        Ok(Ok(msg)) => anyhow::bail!("expected the stream to fail, got {msg:?}"),
        Ok(Err(_)) => {}
    }
    // The dispatch sink meters the fault after the stream's handles drop, so the
    // leaf can see the failure first. Wait for the node-fault counter.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while assert_counter(&b_metrics, "serve_stream_node_fault_total", 1).is_err()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_counter(&b_metrics, "serve_stream_node_fault_total", 1)?;
    assert_counter(&b_metrics, "node_pull_through_client_abandoned_total", 0)?;
    assert_counter(&b_metrics, "serve_stream_proof_budget_exhausted_total", 0)?;

    leaf.conn.close(0u32.into(), b"done");
    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// A leaf that drops while B waits for its proof is a client abandon (#2134).
/// The proof read fails on a peer-attributable transport error, so B meters the
/// abandon. It is neither a node fault nor a spent proof budget.
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_leaf_drop_in_recoup_is_an_abandon() -> Result<()> {
    let payload_len = usize::try_from(CHUNK_BYTES.saturating_mul(3)).unwrap_or(usize::MAX);
    let payload = vec![0x5Eu8; payload_len];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xAB);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x5F);
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, _b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            0,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let leaf = leaf_reads_first_interval(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        leaf_channel_id,
        hash,
    )
    .await?;
    // B has put the first interval on the wire and waits for its proof.
    leaf.conn.close(0u32.into(), b"gone");

    // The inbound stream fails once dispatch sees the error, and dispatch
    // meters any node fault right after that in the same task. Wait for the
    // failure, so the node-fault check below reads a settled value.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while assert_counter(&b_metrics, "streams_failed_total{direction=\"inbound\"}", 1).is_err()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_counter(&b_metrics, "streams_failed_total{direction=\"inbound\"}", 1)?;
    assert_counter(&b_metrics, "node_pull_through_client_abandoned_total", 1)?;
    assert_counter(&b_metrics, "serve_stream_proof_budget_exhausted_total", 0)?;
    assert_counter(&b_metrics, "serve_stream_node_fault_total", 0)?;

    shutdown([task_a, task_b], [&leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_oversized_upstream_aborts_on_received_bytes() -> Result<()> {
    // #1895: the fused serve no longer refuses on the upstream's signed `total_bytes`
    // claim. B signs `ok: true`, opens the upstream pull, and forwards while filling —
    // but the pull leg's receive loop enforces `max_blob_size_bytes` on the bytes that
    // ACTUALLY arrive, so it ABORTS once cumulative received bytes cross the ceiling.
    // The regression this guards: a claim-based refusal would let a holder inflate a
    // small blob's size to make B (and every finite-ceiling relay) refuse to
    // cache/serve while it monopolises the traffic; enforcing on received bytes makes
    // the lie inert while still capping an honest giant. A forgotten tee release on
    // the abort arm would strand the in-flight claim for the hash. We assert: the
    // leaf's fetch fails (B aborted mid-serve), B's upstream spend stays bounded to
    // roughly one ceiling (NOT the whole blob), nothing is promoted into B's cache,
    // the too-large pull counter moves, and the aborted fill releases its tee claim so
    // an identical retry is not wedged.
    let payload = vec![0xB1u8; 4 * 1024 * 1024];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA6);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x6F);
    // 1 MiB ceiling, far below the 4 MiB blob, so the RECEIVED bytes cross it well
    // before the whole blob is pulled. The deposit guard passes against the funded
    // leaf, so we exercise the received-byte cap, not a deposit refusal.
    let max_blob_size_bytes = 1024 * 1024;
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, b_operator) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(DEPOSIT_MICRO_USDC),
            max_blob_size_bytes,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let refused = leaf_paced_pull(
        &leaf_ep,
        b_target.clone(),
        leaf_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused,
        "an upstream blob over the size ceiling must abort the fused serve mid-stream, not complete"
    );

    // Wait for B's pull leg to SETTLE (it writes the upstream watermark this test
    // reads); a sleep that returns first leaves the log empty, which reads as a spend
    // of zero and passes the bound below while proving nothing.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let settled = loop {
        if let Some(&(_, bytes, _)) = progress_log(&recorded)?.last() {
            break bytes;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "B never recorded an upstream watermark: the pull leg did not settle"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "an oversized-upstream abort must not promote the blob into B's cache"
    );
    // B paid the upstream for the received prefix — bounded to the ceiling's own
    // wire, never the whole 4 MiB blob: the receive loop's received-byte ceiling
    // aborts BEFORE the crossing chunk is paid, so the speculative pull window
    // cannot push the spend past it.
    let upstream_bytes: u64 = u64::try_from(settled).unwrap_or(u64::MAX);
    let bound_content = max_blob_size_bytes;
    let bound_wire = support::bao_wire_len(total_bytes, 0, bound_content);
    anyhow::ensure!(
        upstream_bytes > 0,
        "B must pay the upstream for the bytes it actually received before aborting"
    );
    anyhow::ensure!(
        upstream_bytes <= bound_wire,
        "B's upstream spend ({upstream_bytes} wire) must stay bounded to ~one ceiling \
         ({bound_content} content = {bound_wire} wire), not the whole blob"
    );
    assert_counter(&b_metrics, "node_pull_too_large_total", 1)?;

    // The tee claim was released on the abort arm: an identical retry is not wedged on
    // a stranded in-flight entry — it reaches the received-byte cap again and aborts
    // identically (a leaked tee would instead hang/coalesce).
    let retry_sk = fresh_key();
    let retry_node_id = B256::from(*retry_sk.public().as_bytes());
    let (retry_ep, _) = local_endpoint(retry_sk, vec![]).await?;
    let refused_again = leaf_paced_pull(
        &retry_ep,
        b_target,
        retry_node_id,
        &leaf_eth,
        b_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused_again,
        "a repeat request for the same hash must abort again, not wedge on a stranded tee claim"
    );
    assert_counter(&b_metrics, "node_pull_too_large_total", 2)?;

    shutdown([task_a, task_b], [&retry_ep, &leaf_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// Two concurrent misses to ONE provider must both be delivered — the contract
/// `stream_fetch_shared` documents, and which both node pull paths broke by building a
/// fresh `PoolLedger` per pull (#1145 review).
///
/// Nothing exotic is staged here. Node B misses two DIFFERENT blobs at once and both rank
/// the same provider first, which is what an ordinary node on a tens-of-nodes network does
/// all day: the cache engine coalesces in-flight pulls BY HASH, so distinct hashes run
/// concurrent `NodeOrigin::fetch` calls, and both take the channel-REUSE fast path and read
/// the same `prior_amount`.
///
/// With a ledger each, both pulls sign from that same `prior_amount`, so their cumulative
/// amounts collide. Node A — the real `ClientHandler`, enforcing real cumulative
/// monotonicity — accepts the first and rejects the second `AmountRegression`, so one of
/// these two fetches comes back empty. An empty fetch is bad on its own; what makes it a
/// money bug is that `AmountRegression` is TERMINAL once the bounded watermark-resume
/// attempts are spent — it wedges the channel (the row is kept for the reclaim sweep, but
/// the provider is suppressed and the loser's ledger desyncs) — so a collision the shared
/// ledger prevents would otherwise strand the deposit. Hence the assertions beyond "both
/// blobs arrived": nothing is retired, and the recorded cumulative carries EVERY voucher of
/// both pulls on one monotonic sequence.
///
/// The client sends vouchers optimistically and each pull persists the shared ledger's
/// SETTLE-HIGH watermark (#1484), so both `record_progress` calls report the fully advanced
/// cumulative rather than two disjoint sub-watermarks: the evidence of sharing is that the
/// recorded cumulative wire bytes reach both pulls' combined total, which two separate
/// ledgers — each capped at one pull's wire bytes — could never reach.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn two_concurrent_pulls_to_one_provider_share_the_channel_ledger() -> Result<()> {
    let payload = vec![0xC1u8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    // A second blob of the SAME length: the probe responder quotes one `total_bytes`.
    let payload2 = vec![0xC2u8; PAYLOAD_LEN];
    let hash2 = Hash::new(&payload2);
    anyhow::ensure!(hash != hash2, "the two fixtures must be distinct blobs");
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds both blobs, serves the REAL client handler (so vouchers are
    //     validated for real — a colliding nonce is genuinely rejected, not simulated).
    let (cache_a, _tmp_a) = cache_with_blobs(&[payload.as_slice(), payload2.as_slice()]).await?;
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xC7);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
    );

    // --- Node B: one origin, both hashes discoverable on A.
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for h in [hash, hash2] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(a_id).with_ip_addr(addr_a),
            *h.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let (providers, addr_map) = one_provider(a_dht, a_eth.address());

    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let retired: Arc<Mutex<Vec<(Address, B256)>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::clone(&retired),
    }) as Arc<dyn PoolOpener>;

    let (engine, _engine_tmp) = throwaway_engine().await?;
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(DhtNodeId::from_bytes(
            *b_id.as_bytes(),
        )))),
        staker_set: Arc::new(ConfigStakerSet::empty()) as Arc<dyn StakerSet>,
        origin_directory: Arc::new(StaticOriginDirectory::new(HashMap::from([
            (U256::ZERO, providers.clone()),
            (U256::ZERO, providers),
        ]))) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: DhtNodeId::from_bytes(*b_id.as_bytes()),
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(&local_rep),
        negative_cache: NegativeProbeCache::new(),
        probe_cache: PositiveProbeCache::new(),
        metrics: Arc::clone(&b_metrics),
        registry_regions: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        config: NodeOriginConfig {
            probe_fanout: 5,
            // Generous by intent, like [`DEFAULT_TEST_PULL_DEADLINES`]: a loaded
            // runner must not end a pull this fixture is not measuring.
            pull_timeout: DEFAULT_TEST_PULL_DEADLINES.0,
            stall_window: DEFAULT_TEST_PULL_DEADLINES.1,
            min_throughput_bps: 0,
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            // Reactive mid-pull top-up OFF (#1530): this fixture asserts what a pull
            // does when its channel runs dry, which a self-funding one would hide.
            working_deposit: U256::ZERO,
            seller_reserve: U256::ZERO,
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
            serve_economics: std::sync::Arc::new(decdn_node::serve_economics::OffPolicy),
            operator_shares: decdn_node::fee_shares::OperatorShares::new(6000),
            frequency_estimator: None,
            sell_rate_base: 0,
            // A budget far larger than any test's buy cost, so these fixtures warm
            // freely and the ADR 041 gate never changes their behaviour.
            warming: std::sync::Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
                1_000_000_000,
                0,
            )),
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        engine: engine.clone(),
    });

    // The two misses race, exactly as two cache misses for different blobs do.
    // Bound the join so a hung channel-open leaves a clear verdict instead of the
    // opaque nextest `slow-timeout` kill (#1826). The bound is a diagnostic, not a
    // performance assertion: it sits far enough above the honest cost of two
    // channel opens under `cargo llvm-cov` contention that a slow-but-correct run
    // still passes, and far enough below this binary's `60s x 3` nextest cap that
    // the named verdict is what CI reports.
    let join_budget = Duration::from_mins(1);
    let (first, second) = tokio::time::timeout(join_budget, async {
        tokio::join!(
            Origin::fetch(&origin, hash, u64::MAX),
            Origin::fetch(&origin, hash2, u64::MAX),
        )
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "concurrent ledger pulls hung past {join_budget:?} — likely \
             CHANNEL_OPEN_CALLER_BUDGET={CHANNEL_OPEN_CALLER_BUDGET:?} expiry under llvm-cov \
             contention, not AmountRegression; pending={} timeout={} stalled={} recorded={:?} \
             retired={:?}",
            counter_value(&b_metrics, "node_pull_pool_open_pending_total").unwrap_or(0),
            counter_value(&b_metrics, "node_pull_timeout_total").unwrap_or(0),
            counter_value(&b_metrics, "node_pull_stalled_total").unwrap_or(0),
            recorded.lock().map_or_else(|_| Vec::new(), |v| v.clone()),
            retired.lock().map_or_else(|_| Vec::new(), |v| v.clone())
        )
    })?;
    let first = first.map_err(|e| anyhow::anyhow!("first concurrent fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(&first, OriginFetch::AlreadyAdmitted),
        "first concurrent pull returned {first:?} (expected AlreadyAdmitted); AmountRegression \
         would retire the channel and cap cumulative at one pull's wire bytes, while \
         CHANNEL_OPEN_CALLER_BUDGET expiry increments node_pull_pool_open_pending_total and \
         surfaces NotFound — check pending={} timeout={} stalled={} recorded={:?} \
         retired={:?}",
        counter_value(&b_metrics, "node_pull_pool_open_pending_total").unwrap_or(0),
        counter_value(&b_metrics, "node_pull_timeout_total").unwrap_or(0),
        counter_value(&b_metrics, "node_pull_stalled_total").unwrap_or(0),
        recorded.lock().map_or_else(|_| Vec::new(), |v| v.clone()),
        retired.lock().map_or_else(|_| Vec::new(), |v| v.clone())
    );
    let got1 = engine.get(hash).await?;
    let second = second.map_err(|e| anyhow::anyhow!("second concurrent fetch failed: {e}"))?;
    anyhow::ensure!(
        matches!(&second, OriginFetch::AlreadyAdmitted),
        "second concurrent pull returned {second:?} (expected AlreadyAdmitted); AmountRegression \
         would retire the channel and cap cumulative at one pull's wire bytes, while \
         CHANNEL_OPEN_CALLER_BUDGET expiry increments node_pull_pool_open_pending_total and \
         surfaces NotFound — check pending={} timeout={} stalled={} recorded={:?} \
         retired={:?}",
        counter_value(&b_metrics, "node_pull_pool_open_pending_total").unwrap_or(0),
        counter_value(&b_metrics, "node_pull_timeout_total").unwrap_or(0),
        counter_value(&b_metrics, "node_pull_stalled_total").unwrap_or(0),
        recorded.lock().map_or_else(|_| Vec::new(), |v| v.clone()),
        retired.lock().map_or_else(|_| Vec::new(), |v| v.clone())
    );
    let got2 = engine.get(hash2).await?;
    anyhow::ensure!(got1.as_ref() == payload.as_slice(), "blob 1 bytes mismatch");
    anyhow::ensure!(
        got2.as_ref() == payload2.as_slice(),
        "blob 2 bytes mismatch"
    );

    // The collision's real cost: `AmountRegression` is a terminal verdict, so the losing
    // pull would RETIRE the channel the winner is still streaming on.
    let retired_now = retired.lock().expect("retired lock").clone();
    anyhow::ensure!(
        retired_now.is_empty(),
        "no channel may be retired here — both pulls paid honestly on a live channel, got {retired_now:?}"
    );

    // Both pulls draw from ONE shared pool ledger, so their cumulative
    // watermark is a single monotonic sequence covering the wire bytes of both
    // 1.5 MiB pulls. Both settlements persist the shared settle-high watermark,
    // so the sharing shows up as the recorded cumulative reaching the COMBINED
    // two-pull wire total. Two separate ledgers would each cap at one pull's wire
    // bytes and collide on the first voucher — which the empty-result / retire
    // checks above already catch.
    let single_wire =
        decdn_cache::range_pull::bao_encoded_size(total_bytes, &bao_tree::ChunkRanges::all());
    let combined_wire = U256::from(single_wire).saturating_mul(U256::from(2u64));
    let entries = recorded.lock().expect("recorded lock").clone();
    anyhow::ensure!(
        entries.len() == 2,
        "expected one recorded settlement per pull, got {entries:?}"
    );
    let top = entries
        .iter()
        .map(|(_, bytes, ..)| *bytes)
        .max()
        .unwrap_or(U256::ZERO);
    anyhow::ensure!(
        top == combined_wire,
        "the shared ledger's cumulative must carry both pulls' wire bytes \
         ({combined_wire}); separate ledgers would each cap at one pull's {single_wire}: \
         {entries:?}"
    );

    shutdown([task_a], [&ep_a]).await?;
    Ok(())
}

/// ADR 001 §Probe cache: "On a cache miss the requester checks the probe cache first; if a
/// valid entry exists, it skips DHT lookup and goes straight to selection."
///
/// Observed where it is defined — at the WIRE. Asserting on
/// `decdn_probe_cache_hits_total` alone would pass an implementation that increments the
/// counter and probes anyway; the counter is the report, the silent probe endpoint is the
/// property.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_second_fetch_inside_the_ttl_skips_the_probe_entirely() -> Result<()> {
    let payload = vec![0x5Cu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client, counting probes. ------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x5C);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let probes = Arc::new(AtomicUsize::new(0));
    let task_a = spawn_a_probe_counting_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        Arc::clone(&probes),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    // The priming dial above is itself a real probe against A's counting server
    // — reset so the counter below measures only the fetches under test.
    probes.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, engine, _recorded, _engine_tmp) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        pool_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    )
    .await;

    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "first fetch must deliver the blob"
    );
    anyhow::ensure!(probes.load(Ordering::SeqCst) == 1, "first fetch must probe");
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "second fetch must deliver the blob"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 1,
        "the second fetch re-probed — the probe cache saved nothing, which is the entire \
         point of ADR 001 §Probe cache"
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    // ONE orchestration per fetch, however many candidate lists it walks.
    assert_counter(&b_metrics, "node_pull_attempts_total", 2)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// The TTL is not decorative. Past it the entry is gone and the fetch pays for a fresh
/// lookup + probe — which is what bounds how stale a served candidate can be. Uses an
/// injected 300ms TTL via `build_origin_with_probe_caches`: the real 15s is anchored on
/// `Instant`, so `tokio::time` cannot skip it.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_fetch_past_the_ttl_probes_again() -> Result<()> {
    let payload = vec![0x7Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client, counting probes. ------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x7E);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let probes = Arc::new(AtomicUsize::new(0));
    let task_a = spawn_a_probe_counting_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        Arc::clone(&probes),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    // The priming dial above is itself a real probe against A's counting server
    // — reset so the counter below measures only the fetches under test.
    probes.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let b_buyer2 = Arc::clone(&b_buyer);
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: b_buyer2,
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    // A 300ms positive-cache TTL: short enough that a real sleep past it is not a
    // test-suite hazard, unlike the production 15s (anchored on `Instant`, so
    // `tokio::time::pause` cannot fast-forward it).
    let (origin, engine, _engine_tmp) = build_origin_with_probe_caches(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
        NegativeProbeCache::new(),
        PositiveProbeCache::with_capacity_and_ttl(16, Duration::from_millis(300)),
        // Buffered `Origin::fetch` path — directory keyed under `NO_NAMESPACE`.
        U256::ZERO,
        // Reactive mid-pull top-up off; only the #1530 tests turn it on.
        U256::ZERO,
    )
    .await;

    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "first fetch must deliver the blob"
    );
    anyhow::ensure!(probes.load(Ordering::SeqCst) == 1, "first fetch must probe");

    // Sleep well past the 300ms TTL — generous margin, as the negative-cache
    // TTL-inversion tests use.
    tokio::time::sleep(Duration::from_millis(900)).await;

    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "second fetch must deliver the blob"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 2,
        "a fetch past the TTL must probe again — the entry is gone, got {} probes",
        probes.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;
    assert_counter(&b_metrics, "probe_cache_hits_total", 0)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// ONE `MAX_PROVIDER_ATTEMPTS` budget for the whole fetch (#1165), not one per phase.
///
/// Three cached providers that all refuse must consume the budget and END the fetch — not
/// hand a fresh lookup three more pulls. Six sequential pulls against an
/// `outer_pull_deadline` sized for three (172s at defaults) is a fetch killed mid-pull by a
/// timeout that no longer describes it, and nothing in `selection.rs` would catch it: the
/// deadline is enforced elsewhere, on the assumption this constant is honoured here.
///
/// A third fetch then observes the OTHER half of the exhaustion contract: the entry the
/// second fetch disproved must be INVALIDATED, so the next fetch goes cold rather than
/// re-walking a list whose every member just failed. Without this fetch, deleting the
/// `probe_cache.invalidate(&target)` call in `Origin::fetch` left the whole suite green
/// (#1223 review): nothing distinguished "entry dropped" from "entry kept but every hit
/// spends the budget the same way" until something asked for the hash a third time.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn cached_candidates_and_the_cold_path_share_one_attempt_budget() -> Result<()> {
    let payload = vec![0x3Bu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let probes = Arc::new(AtomicUsize::new(0));
    let streams = Arc::new(AtomicUsize::new(0));

    // --- Three providers: reachable, honest at probe, refuse the pull with
    //     `InternalError` — reported node-fault, so NOT negative-cached and NOT
    //     wedged, which is exactly what lets all three remain in the cache for
    //     the second fetch to spend its budget on. -----------------------------
    let n0_sk = fresh_key();
    let n0_id = n0_sk.public();
    let n0_eth = Arc::new(PrivateKeySigner::random());
    let (ep_n0, addr_n0) =
        local_endpoint(n0_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_n0 = spawn_a_refusing_server_with_counters(
        ep_n0.clone(),
        Arc::clone(&n0_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::InternalError,
        Arc::clone(&probes),
        Arc::clone(&streams),
    );

    let n1_sk = fresh_key();
    let n1_id = n1_sk.public();
    let n1_eth = Arc::new(PrivateKeySigner::random());
    let (ep_n1, addr_n1) =
        local_endpoint(n1_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_n1 = spawn_a_refusing_server_with_counters(
        ep_n1.clone(),
        Arc::clone(&n1_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::InternalError,
        Arc::clone(&probes),
        Arc::clone(&streams),
    );

    let n2_sk = fresh_key();
    let n2_id = n2_sk.public();
    let n2_eth = Arc::new(PrivateKeySigner::random());
    let (ep_n2, addr_n2) =
        local_endpoint(n2_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_n2 = spawn_a_refusing_server_with_counters(
        ep_n2.clone(),
        Arc::clone(&n2_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::InternalError,
        Arc::clone(&probes),
        Arc::clone(&streams),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(n0_id, addr_n0), (n1_id, addr_n1), (n2_id, addr_n2)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }
    // The priming dials above are themselves real probes against the counting
    // servers — reset so the counter below measures only the fetches under test.
    probes.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());

    let n0_dht = DhtNodeId::from_bytes(*n0_id.as_bytes());
    let n1_dht = DhtNodeId::from_bytes(*n1_id.as_bytes());
    let n2_dht = DhtNodeId::from_bytes(*n2_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(n0_dht, n0_eth.address());
    addr_map.insert(n1_dht, n1_eth.address());
    addr_map.insert(n2_dht, n2_eth.address());

    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x3B),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;

    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        vec![n0_dht, n1_dht, n2_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    // Fetch #1: cold path. Discovers, probes, and ranks all three; the whole
    // MAX_PROVIDER_ATTEMPTS budget is spent refusing, and the ranked list is
    // cached at `probe_and_rank`'s tail regardless of the miss.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::NotFound),
        "all three providers refuse; the first fetch must miss"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 3,
        "expected all three providers probed exactly once, got {}",
        probes.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 3)?;

    // Fetch #2: the probe-cache hit walks the SAME three cached candidates (none
    // negative-cached or wedged by an `InternalError` refusal). It must spend
    // exactly the fetch-wide budget of 3 — not hand a fresh lookup three MORE —
    // and must not re-probe anyone.
    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::NotFound),
        "the cached candidates all refuse again; the second fetch must miss too"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 3,
        "the second fetch re-probed — the cache hit must supply the whole budget without a \
         fresh lookup, got {} probes",
        probes.load(Ordering::SeqCst)
    );
    // Exactly 3 MORE refusals (6 total) — not 6 more (9 total), which is what a
    // fresh lookup on top of the cached attempts would have produced.
    assert_counter(&b_metrics, "node_pull_refused_total", 6)?;
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    // Fetch #3: fetch #2 disproved the cached entry by spending the whole budget
    // on it, so it must have been INVALIDATED — none of the three refusers is
    // negative-cached or wedged (`InternalError`), so only the invalidate stands
    // between this fetch and a second hit on the same dead list. It must go COLD:
    // a fresh lookup that re-probes all three at the wire.
    let third = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("third fetch: {e}"))?;
    anyhow::ensure!(
        matches!(third, OriginFetch::NotFound),
        "all three providers still refuse; the third fetch must miss too"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 6,
        "the third fetch must re-probe all three providers — the exhausted entry was \
         disproved by real pulls and must not survive to hit again, got {} probes",
        probes.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;
    assert_counter(&b_metrics, "node_pull_refused_total", 9)?;
    // Still one orchestration per fetch: cold, hit-exhausted, cold again.
    assert_counter(&b_metrics, "node_pull_attempts_total", 3)?;

    shutdown([task_n0, task_n1, task_n2], [&ep_b, &ep_n0, &ep_n1, &ep_n2]).await?;
    Ok(())
}

/// A cache hit whose (fewer-than-budget) cached candidates ALL fail must still fall through
/// to the cold path WITHIN THE SAME fetch, and that fetch must still meter
/// `node_pull_attempt()` exactly once (#1165 review).
///
/// `cached_candidates_and_the_cold_path_share_one_attempt_budget` (above) proves the budget is
/// shared, but its cached phase spends the WHOLE budget (3 refusers) and never reaches the
/// cold path within the second fetch. This test hits the other branch: a cache with FEWER than
/// [`decdn_node::selection::MAX_PROVIDER_ATTEMPTS`] candidates, all of which fail, so `budget`
/// stays positive and the fetch falls through to a fresh discovery — the only path that
/// exercises `fetch`'s `if !attempt_metered` skip. If that guard regressed to always firing,
/// this exact fetch would double-count `node_pull_attempts_total`.
///
/// Provider N refuses every pull but answers probes honestly, so it is cached after fetch #1
/// yet fails again on fetch #2's cached-phase attempt. Provider H is a real, healthy upstream
/// that is *unreachable* (no bound endpoint at all, mirroring
/// `node_origin_probe_unreachable_is_scored`'s "dialing it fails fast") during fetch #1 — so it
/// is never probed, never becomes a cached candidate, and is never negative-cached — then comes
/// online between the two fetches. H quotes a far cheaper rate so it always ranks ahead of N
/// once both are probed fresh, guaranteeing fetch #2's cold path tries H first and delivers
/// without retrying N.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_partial_cached_budget_falls_through_to_the_cold_path_and_meters_once() -> Result<()> {
    let payload = vec![0x1Bu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    // --- Provider N: reachable, honest at probe, refuses the pull with
    //     `InternalError` — reported node-fault, so NOT negative-cached and NOT
    //     wedged, exactly like the shared-budget test's three refusers. -----------
    let probes_n = Arc::new(AtomicUsize::new(0));
    let streams_n = Arc::new(AtomicUsize::new(0));
    let n_sk = fresh_key();
    let n_id = n_sk.public();
    let n_eth = Arc::new(PrivateKeySigner::random());
    let (ep_n, addr_n) =
        local_endpoint(n_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_n = spawn_a_refusing_server_with_counters(
        ep_n.clone(),
        Arc::clone(&n_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::InternalError,
        Arc::clone(&probes_n),
        Arc::clone(&streams_n),
    );

    // --- Provider H: identity only for now — NO endpoint bound yet, so it is
    //     undiscoverable (dialing it fails fast, like a fresh identity with no
    //     server) until it comes online after fetch #1. ------------------------
    let h_sk = fresh_key();
    let h_id = h_sk.public();
    let h_eth = Arc::new(PrivateKeySigner::random());

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;

    // Prime ep_b's address book for N only — H has no address to learn yet.
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(n_id).with_ip_addr(addr_n),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    // The priming dial above is itself a real probe against N's counting server
    // — reset so the counters below measure only the fetches under test.
    probes_n.store(0, Ordering::SeqCst);
    streams_n.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x1B);

    let n_dht = DhtNodeId::from_bytes(*n_id.as_bytes());
    let h_dht = DhtNodeId::from_bytes(*h_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(n_dht, n_eth.address());
    addr_map.insert(h_dht, h_eth.address());

    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;

    let (origin, engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        vec![n_dht, h_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    // Fetch #1: cold path (cache empty). H is undiscoverable, so only N is
    // probed and cached — a SINGLE cached candidate, fewer than
    // `MAX_PROVIDER_ATTEMPTS`. N refuses, so the fetch is a clean miss.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::NotFound),
        "H is unreachable and N refuses; the first fetch must miss"
    );
    anyhow::ensure!(
        probes_n.load(Ordering::SeqCst) == 1,
        "expected N probed exactly once, got {}",
        probes_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        streams_n.load(Ordering::SeqCst) == 1,
        "expected N pulled-from exactly once, got {}",
        streams_n.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    // 2, not 1: H's failed probe scores Unreachable, AND N's `InternalError` refusal is
    // classified `RefusalVerdict::NodeFault`, which — per `classify_pull_failure` — ALSO
    // scores `Outcome::Unreachable` on top of incrementing `node_pull_refused_total` (a
    // refusal proves reachability at the wire but is still reputation-scored as
    // unreachable). This is why the shared-budget test above never asserts this counter.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 2)?;
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;

    // --- Bring H online between the two fetches: a real, healthy upstream
    //     holding the blob, quoting a far cheaper rate so it always outranks N
    //     once both are probed fresh. -------------------------------------------
    let (cache_h, hash_h, _tmp_h) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_h == hash, "fixture hash mismatch");
    let store_h = Arc::new(MemoryPoolStateStore::new());
    store_h.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        h_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_h_side = Arc::new(Metrics::new());
    let limiter_h = permissive_limiter(&metrics_h_side);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let h_rate = 1u64;
    let handler_h = build_handler_full(
        h_id,
        &h_eth,
        &metrics_h_side,
        limiter_h,
        cache_h,
        store_h as Arc<dyn PoolStateStore>,
        h_rate,
        &domains,
        16,
    )?;
    let (ep_h, addr_h) =
        local_endpoint(h_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let probes_h = Arc::new(AtomicUsize::new(0));
    let task_h = spawn_a_probe_counting_server(
        ep_h.clone(),
        handler_h,
        Arc::clone(&h_eth),
        slash_domain(),
        total_bytes,
        h_rate,
        Arc::clone(&probes_h),
    );

    // Prime ep_b's address book for H now that it is listening.
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(h_id).with_ip_addr(addr_h),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    // Again, the priming dial is itself a real probe — reset before the fetch
    // under test.
    probes_h.store(0, Ordering::SeqCst);

    // Fetch #2: cache hit on N alone. The cached phase spends 1 of the 3-attempt
    // budget on N (refused again), leaving budget > 0, so the fetch falls
    // through — WITHIN THIS CALL — to a fresh lookup that discovers both N and
    // H, ranks H first (cheaper rate), and delivers via H without retrying N.
    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "the second fetch must deliver the blob via the cold fallthrough"
    );
    anyhow::ensure!(
        streams_n.load(Ordering::SeqCst) == 2,
        "N must be pulled-from exactly twice total (once per fetch) — the cold path must not \
         retry it once H (ranked first) delivers, got {}",
        streams_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        probes_n.load(Ordering::SeqCst) == 2,
        "N must be re-probed by the fresh cold-path lookup (the cache hit itself never probes), \
         got {}",
        probes_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        probes_h.load(Ordering::SeqCst) == 1,
        "H must be probed exactly once, by the cold-path fallthrough, got {}",
        probes_h.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    assert_counter(&b_metrics, "node_pull_refused_total", 2)?;
    // 3, not 2: the 2 from fetch #1 (H unreachable + N's NodeFault refusal) plus one more
    // from N's second (cached-phase) refusal. H's successful cold-path probe and delivery
    // add none.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 3)?;
    // THE property under test: one orchestration per fetch, however many
    // candidate lists it walks THIS TIME — 2, not 3. A regressed
    // `if !attempt_metered` guard that always fires would double-count fetch
    // #2's cold-path fallthrough and push this to 3.
    assert_counter(&b_metrics, "node_pull_attempts_total", 2)?;

    shutdown([task_n, task_h], [&ep_b, &ep_n, &ep_h]).await?;
    Ok(())
}

/// A cached entry can go stale INSIDE its own 15s. A pull-time refusal recorded a negative
/// for this (peer, hash) seconds ago, and the hit path must honour it — otherwise the
/// positive cache resurrects exactly the peers the last fetch just learned not to ask,
/// which is a positive cache that undoes the negative one.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_probe_cache_hit_still_honours_the_negative_cache() -> Result<()> {
    let payload = vec![0x9Du8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node N: refuses every pull with NotFound (honest, negative-cacheable),
    //     quoting a cheaper rate so it ranks #1 and is always tried first. -------
    let n_sk = fresh_key();
    let n_id = n_sk.public();
    let n_eth = Arc::new(PrivateKeySigner::random());
    let (ep_n, addr_n) =
        local_endpoint(n_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let n_probes = Arc::new(AtomicUsize::new(0));
    let n_streams = Arc::new(AtomicUsize::new(0));
    let task_n = spawn_a_refusing_server_with_counters(
        ep_n.clone(),
        Arc::clone(&n_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
        StreamError::NotFound,
        Arc::clone(&n_probes),
        Arc::clone(&n_streams),
    );

    // --- Node A: holds the blob; serves probe + client honestly. --------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x9D);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let a_probes = Arc::new(AtomicUsize::new(0));
    let task_a = spawn_a_probe_counting_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        Arc::clone(&a_probes),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(n_id, addr_n), (a_id, addr_a)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }
    // The priming dials above are themselves real probes against the counting
    // servers — reset so the counters below measure only the fetches under test.
    n_probes.store(0, Ordering::SeqCst);
    a_probes.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let n_dht = DhtNodeId::from_bytes(*n_id.as_bytes());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(n_dht, n_eth.address());
    addr_map.insert(a_dht, a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        vec![n_dht, a_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    // Fetch #1: cold path. N is tried first (cheaper rate), refuses NotFound
    // (negative-cached for `REFUSAL_SUPPRESSION_TTL`), and the loop falls
    // through to A, which delivers. Both are probed once and the ranked [N, A]
    // pair is cached at `probe_and_rank`'s tail.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "first fetch must deliver the blob from A"
    );
    anyhow::ensure!(
        n_probes.load(Ordering::SeqCst) == 1,
        "N must be probed exactly once, got {}",
        n_probes.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        n_streams.load(Ordering::SeqCst) == 1,
        "N must be pulled exactly once, got {}",
        n_streams.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;

    // Fetch #2: the probe-cache hit walks the cached [N, A] pair, but N is
    // still negative-cached from its refusal seconds ago — `cached_candidates`
    // must filter it out, leaving only A. If it did not, N would be re-tried
    // and re-refuse: exactly the positive cache undoing the negative one.
    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "second fetch must still deliver the blob from A"
    );
    anyhow::ensure!(
        n_probes.load(Ordering::SeqCst) == 1,
        "the cache hit re-probed N — the negative cache must be honoured on a hit, got {}",
        n_probes.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        n_streams.load(Ordering::SeqCst) == 1,
        "the cache hit re-streamed N — the negative cache must be honoured on a hit, got {}",
        n_streams.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    shutdown([task_n, task_a], [&ep_b, &ep_n, &ep_a]).await?;
    Ok(())
}

/// A cache entry whose EVERY provider is currently suppressed is a miss, not a hit —
/// `cached_candidates`' `candidates.is_empty() → None` branch (#1223 review).
///
/// The distinction is not cosmetic. A hit meters `probe_cache_hits_total` and — when its
/// (empty) candidate walk "fails" — INVALIDATES the entry; a miss meters
/// `probe_cache_misses_total` and leaves the entry alone. If the branch regressed to
/// `Some(vec![])`, every fetch inside a suppression window would count a hit that saved no
/// work (the dashboard lie) and throw away an entry whose providers may be perfectly good
/// once the suppression lapses (the behavioural one). The negative-cache-interaction test
/// above cannot see this: its entry keeps a second, unsuppressed provider, so the filtered
/// list is never empty.
///
/// Sole provider N refuses with `EvictedSinceProbe` — durable, so fetch #1 negative-caches
/// the (peer, hash) for the full cache-wide TTL while the positive entry (written at
/// `probe_and_rank`'s tail regardless) survives. Fetch #2 finds the entry, filters N out,
/// and must take the MISS path end to end: no hit metered, and no re-probe or re-stream of
/// N at the wire (the cold path's own fanout filter drops the suppressed peer too — the two
/// chokepoints agreeing is the point).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn an_entry_whose_every_provider_is_suppressed_is_a_miss_not_a_hit() -> Result<()> {
    let payload = vec![0x2Fu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    // --- Node N: honest at probe, refuses every pull with `EvictedSinceProbe` —
    //     durable, so the refusal suppresses this (peer, hash) for the FULL
    //     cache-wide TTL (5 min at defaults), comfortably covering fetch #2. ------
    let probes_n = Arc::new(AtomicUsize::new(0));
    let streams_n = Arc::new(AtomicUsize::new(0));
    let n_sk = fresh_key();
    let n_id = n_sk.public();
    let n_eth = Arc::new(PrivateKeySigner::random());
    let (ep_n, addr_n) =
        local_endpoint(n_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_n = spawn_a_refusing_server_with_counters(
        ep_n.clone(),
        Arc::clone(&n_eth),
        slash_domain(),
        total_bytes,
        RATE,
        StreamError::EvictedSinceProbe,
        Arc::clone(&probes_n),
        Arc::clone(&streams_n),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(n_id).with_ip_addr(addr_n),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    // The priming dial above is itself a real probe against N's counting server
    // — reset so the counters below measure only the fetches under test.
    probes_n.store(0, Ordering::SeqCst);
    streams_n.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*n_id.as_bytes()), n_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id: B256::repeat_byte(0x2F),
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, _engine, _engine_tmp) = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    )
    .await;

    // Fetch #1: cold path. N is probed, ranked, cached — and its refusal
    // negative-caches (N, hash) for the full TTL, stranding the fresh positive
    // entry with no usable provider.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::NotFound),
        "N refuses; the first fetch must miss"
    );
    anyhow::ensure!(
        probes_n.load(Ordering::SeqCst) == 1,
        "expected N probed exactly once, got {}",
        probes_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        streams_n.load(Ordering::SeqCst) == 1,
        "expected N pulled-from exactly once, got {}",
        streams_n.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;

    // Fetch #2: the entry is found but N — its only provider — is filtered by the
    // negative cache, so `cached_candidates` must return None and the fetch must
    // take the MISS path: no hit metered, and the cold lookup's own fanout filter
    // drops the suppressed N before it can be re-probed or re-streamed.
    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::NotFound),
        "N is still suppressed; the second fetch must miss"
    );
    // THE property under test. A regressed `Some(vec![])` would meter this fetch
    // as a hit (1/1 instead of 0/2) — a "hit" that saved no network work — and
    // invalidate an entry the suppression, not the providers, made unusable.
    assert_counter(&b_metrics, "probe_cache_hits_total", 0)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;
    anyhow::ensure!(
        probes_n.load(Ordering::SeqCst) == 1,
        "the suppressed N must not be re-probed by either phase of fetch #2, got {}",
        probes_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        streams_n.load(Ordering::SeqCst) == 1,
        "the suppressed N must not be re-streamed by fetch #2, got {}",
        streams_n.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    // Both fetches found ≥1 provider at discovery, so both are orchestrations —
    // and neither is a `no_providers` (that would break the metrics' mutual
    // exclusivity).
    assert_counter(&b_metrics, "node_pull_attempts_total", 2)?;
    assert_counter(&b_metrics, "node_pull_no_providers_total", 0)?;

    shutdown([task_n], [&ep_b, &ep_n]).await?;
    Ok(())
}

/// A probe-cache hit must honour the WEDGED-provider filter, not just the negative cache —
/// `cached_candidates`' `provider_is_wedged` branch (#1223 review).
///
/// `node_origin_a_wedged_provider_is_skipped_for_other_hashes` guards this filter at
/// `probe_and_rank` (the cold chokepoint); this test guards it at the other chokepoint. The
/// negative-cache-interaction test above cannot: a wedge also negative-caches the SAME
/// (peer, hash) for 30s, so for the hash that wedged the channel the negative branch
/// short-circuits and the wedged branch is dead code to it. The hole needs a hash the
/// provider was cached for BEFORE its channel wedged on a different one — exactly what a
/// popular blob inside its 15s TTL looks like when some other pull discovers the dead
/// channel.
///
/// Provider A is cached for hash2 (fetch #1, where healthy H outranks it and delivers),
/// then wedges on a pull for hash1 (fetch #2: A rejects the closing voucher with
/// `AmountRegression` → `OurDeadLane` → provider-wide suppression for a fixed window).
/// (A, hash2) is never negative-cached, so when H goes offline and fetch #3 hits the hash2
/// entry, ONLY the wedged filter stands between A and a lane this node cannot pay on. The
/// wire-level assertion is A's stream counter: still exactly one open ever.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_probe_cache_hit_still_honours_the_wedged_provider_filter() -> Result<()> {
    // Two distinct single-bao-group blobs (4096B, so the closing voucher fires
    // cleanly — see the ack-wait test): hash1 is the wedge bait A holds, hash2 is
    // the blob whose cached entry is under test.
    let payload1 = vec![0x33u8; 4096];
    let payload2 = vec![0x44u8; 4096];
    let hash1 = Hash::new(&payload1);
    let hash2 = Hash::new(&payload2);

    // --- Node A: honest at probe for everything, serves payload1, then rejects
    //     the closing voucher with `AmountRegression` — the wedge trigger. -------
    let probes_a = Arc::new(AtomicUsize::new(0));
    let streams_a = Arc::new(AtomicUsize::new(0));
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_voucher_rejecting_server_with_counters(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload1.clone(),
        u64::try_from(payload1.len()).unwrap_or(u64::MAX),
        RATE,
        VoucherRejectReason::AmountRegression,
        Arc::clone(&probes_a),
        Arc::clone(&streams_a),
    );

    // --- Node H: holds payload2, serves honestly, quotes a far cheaper rate so
    //     it outranks A whenever both are viable — which is what keeps A un-pulled
    //     (and so un-suppressed) for hash2 until the moment under test. -----------
    let (cache_h, hash_h, _tmp_h) = cache_with_blob(&payload2).await?;
    anyhow::ensure!(hash_h == hash2, "fixture hash mismatch");
    let h_sk = fresh_key();
    let h_id = h_sk.public();
    let h_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0x3D);
    let store_h = Arc::new(MemoryPoolStateStore::new());
    store_h.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        h_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_h_side = Arc::new(Metrics::new());
    let limiter_h = permissive_limiter(&metrics_h_side);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let h_rate = 1u64;
    let handler_h = build_handler_full(
        h_id,
        &h_eth,
        &metrics_h_side,
        limiter_h,
        cache_h,
        store_h as Arc<dyn PoolStateStore>,
        h_rate,
        &domains,
        16,
    )?;
    let (ep_h, addr_h) =
        local_endpoint(h_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let probes_h = Arc::new(AtomicUsize::new(0));
    let task_h = spawn_a_probe_counting_server(
        ep_h.clone(),
        handler_h,
        Arc::clone(&h_eth),
        slash_domain(),
        u64::try_from(payload2.len()).unwrap_or(u64::MAX),
        h_rate,
        Arc::clone(&probes_h),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(a_id, addr_a), (h_id, addr_h)] {
        let _ = probe_once(
            &ep_b,
            EndpointAddr::new(id).with_ip_addr(addr),
            *hash2.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }
    // The priming dials above are themselves real probes against the counting
    // servers — reset so the counters below measure only the fetches under test.
    probes_a.store(0, Ordering::SeqCst);
    streams_a.store(0, Ordering::SeqCst);
    probes_h.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let h_dht = DhtNodeId::from_bytes(*h_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(a_dht, a_eth.address());
    addr_map.insert(h_dht, h_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_multi_hash(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        &[hash1, hash2],
        buyer,
        &local_rep,
        &b_metrics,
        &[a_dht, h_dht],
        addr_map,
        // Short pull/stall deadlines: fetch #3 dials the offline H at an address
        // it KNOWS (unlike the fallthrough tests' never-primed identities, a dead
        // UDP addr times out rather than refusing), and that wait is bounded by
        // these. The pulls that matter complete in milliseconds.
        Duration::from_secs(3),
        Duration::from_secs(3),
    )
    .await;

    // Fetch #1 (hash2): cold path. Both are probed and cached in the hash2 entry;
    // H (cheaper) ranks first and delivers, so A is never pulled — no negative
    // entry, no wedge, just a live cached candidate.
    let first = Origin::fetch(&origin, hash2, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::AlreadyAdmitted)
            && engine.get(hash2).await?.as_ref() == payload2.as_slice(),
        "fetch #1 must deliver hash2 from H"
    );
    anyhow::ensure!(
        streams_a.load(Ordering::SeqCst) == 0,
        "A must not be pulled for hash2 while H (ranked first) delivers, got {}",
        streams_a.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_pool_wedged_total", 0)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    // Fetch #2 (hash1): H is tried first and honestly refuses (its cache holds
    // only payload2); A then serves payload1 but rejects the closing voucher —
    // `OurDeadLane`, wedging A provider-wide for the suppression window.
    let second = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash1, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch #2 never ended"))?
    .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::NotFound),
        "fetch #2 must refuse: H lacks hash1 and A cannot be paid"
    );
    // One logical pull to A opens ONE connection: `pull_from_candidate`'s
    // whole-blob header handshake is also the drive's first and only leg, which
    // adopts it (#2063). One stream here means "A WAS pulled once (the hash1
    // wedge)".
    anyhow::ensure!(
        streams_a.load(Ordering::SeqCst) == 1,
        "A must be pulled exactly once (the handshake the drive adopts, the hash1 wedge), \
         got {}",
        streams_a.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_pool_wedged_total", 1)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 1)?;
    let probes_a_after_wedge = probes_a.load(Ordering::SeqCst);

    // --- Take H offline: the hash2 entry now reads [H (dead), A (wedged)], and
    //     only the wedged filter keeps fetch #3 from re-selecting A on a lane this
    //     node cannot pay on. ---------------------------------------------------------
    shutdown([task_h], [&ep_h]).await?;

    // Fetch #3 (hash2, inside the entry's TTL): a probe-cache HIT — H survives the
    // filters, is dialled, and fails fast. A must NOT be the fallback: it is
    // wedged, and (A, hash2) was never negative-cached, so a regressed
    // `provider_is_wedged` branch in `cached_candidates` re-selects it here and
    // opens a stream against a channel that cannot pay.
    let third = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash2, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch #3 never ended"))?
    .map_err(|e| anyhow::anyhow!("third fetch: {e}"))?;
    anyhow::ensure!(
        matches!(third, OriginFetch::NotFound),
        "fetch #3 must miss: H is offline and A is wedged"
    );
    // THE property under test, at the wire: A's only pull ever is the hash1
    // wedge (the 1 stream above). A climb past 1 here is the hit path
    // re-selecting the wedged provider.
    anyhow::ensure!(
        streams_a.load(Ordering::SeqCst) == 1,
        "the cache hit re-streamed the WEDGED A — `cached_candidates` must filter a wedged \
         provider even when its (peer, hash) pair was never negative-cached, got {}",
        streams_a.load(Ordering::SeqCst)
    );
    // …and A is not re-probed either: the cold fallthrough's own wedged filter
    // (guarded by the sibling test) drops it before the probe fanout.
    anyhow::ensure!(
        probes_a.load(Ordering::SeqCst) == probes_a_after_wedge,
        "the wedged A must not be re-probed, got {} (was {probes_a_after_wedge})",
        probes_a.load(Ordering::SeqCst)
    );
    // No second wedge event — the sibling cold-path test's load-bearing counter,
    // asserted here for the hit path.
    assert_counter(&b_metrics, "node_pull_pool_wedged_total", 1)?;
    // The entry WAS consulted (H survived the filters), so fetch #3 is a hit.
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

/// A [`StakerSet`] whose active set can be mutated mid-test, so a fetch can eject a
/// provider between the probe-cache write and a later hit inside the same TTL
/// (#1223). Models a chain ejection the cold path's `filter_active_stakers` would
/// deny — the hit path (`cached_candidates`) must deny it too.
#[derive(Debug)]
struct MutableStakerSet {
    active: Mutex<HashSet<DhtNodeId>>,
}

impl MutableStakerSet {
    const fn new(active: HashSet<DhtNodeId>) -> Self {
        Self {
            active: Mutex::new(active),
        }
    }

    /// Eject `node_id` from the active set — the chain-atomic drop a fetch inside
    /// the TTL must observe on the hit path.
    fn remove(&self, node_id: &DhtNodeId) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(node_id);
    }
}

impl StakerSet for MutableStakerSet {
    fn is_active(&self, node_id: &DhtNodeId) -> bool {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(node_id)
    }

    fn active_nodes(&self) -> Vec<DhtNodeId> {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }
}

/// An [`OriginDirectory`] whose hash → origins map can be mutated mid-test, so a
/// fetch can drop the fallback entry alongside the staker ejection (#1223). Removing
/// the entry is what stops the cold fall-through — after the hit-path skip — from
/// re-serving the ejected provider, isolating the hit-path filter as the thing
/// under test.
#[derive(Debug)]
struct MutableOriginDirectory {
    origins: Mutex<HashMap<U256, Vec<DhtNodeId>>>,
}

impl MutableOriginDirectory {
    const fn new(origins: HashMap<U256, Vec<DhtNodeId>>) -> Self {
        Self {
            origins: Mutex::new(origins),
        }
    }

    /// Drop `namespace`'s entry — the directory half of a chain ejection.
    fn remove(&self, namespace: U256) {
        self.origins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&namespace);
    }
}

#[async_trait]
impl OriginDirectory for MutableOriginDirectory {
    async fn lookup_origins(&self, namespace_id: U256) -> Vec<DhtNodeId> {
        self.origins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&namespace_id)
            .cloned()
            .unwrap_or_default()
    }
}

/// Like [`spawn_a_probe_counting_server`], but also counts client-ALPN opens in
/// `streams`, so a test can witness AT THE WIRE whether A was asked to serve. A
/// serves the real blob through `client_handler`; the stream counter is what proves
/// a provider dropped on a probe-cache hit sees NO new client stream (#1223).
#[allow(clippy::too_many_arguments)]
fn spawn_a_serving_server_with_counters(
    ep: iroh::Endpoint,
    client_handler: Arc<decdn_node::handlers::client::ClientHandler>,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    total_bytes: u64,
    rate: u64,
    probes: Arc<AtomicUsize>,
    streams: Arc<AtomicUsize>,
) -> tokio::task::JoinHandle<()> {
    use iroh::protocol::ProtocolHandler;
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            if conn.alpn() == ALPN_PROBE {
                let eth = Arc::clone(&a_eth);
                let dom = slash.clone();
                let probes = Arc::clone(&probes);
                tokio::spawn(async move {
                    probes.fetch_add(1, Ordering::SeqCst);
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                let handler = Arc::clone(&client_handler);
                let streams = Arc::clone(&streams);
                tokio::spawn(async move {
                    streams.fetch_add(1, Ordering::SeqCst);
                    let _ = decdn_node::handlers::client::ClientProtocol::new(handler)
                        .accept(conn)
                        .await;
                });
            }
        }
    })
}

/// A cached entry can be disowned INSIDE its own 15s: a node ejected from the active
/// set (chain ejection / unbonding) while its probe-cache entry is still live. The
/// cold path denies it via `filter_active_stakers`; the hit path bypasses
/// `find_providers`, so it must re-apply `staker_set.is_active` itself (#1223). This
/// proves that re-check FIRES — the ejected provider is skipped on the hit and NOT
/// re-served at the wire — rather than winning a paid pull the cold path would deny.
///
/// The provider is ejected from BOTH the staker set and the origin directory, as a
/// chain ejection is atomic across the two. The directory removal is load-bearing:
/// without it the cold fall-through (after the hit-path skip) would re-serve A
/// through the unfiltered directory, masking whether the hit-path check ran at all.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_probe_cache_hit_drops_a_provider_no_longer_admitted() -> Result<()> {
    let payload = vec![0xA7u8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client, counting BOTH so a hit
    //     that wrongly re-serves A is visible at the wire. --------------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let pool_id = B256::repeat_byte(0xA7);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let a_probes = Arc::new(AtomicUsize::new(0));
    let a_streams = Arc::new(AtomicUsize::new(0));
    let task_a = spawn_a_serving_server_with_counters(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        RATE,
        Arc::clone(&a_probes),
        Arc::clone(&a_streams),
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    // Prime B's address book so it can dial A by node-id during the fetch; this
    // dial is itself a real probe, so reset the counters after it.
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;
    a_probes.store(0, Ordering::SeqCst);
    a_streams.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let a_dht = DhtNodeId::from_bytes(*a_id.as_bytes());
    let b_dht = DhtNodeId::from_bytes(*b_id.as_bytes());

    // A starts admitted: in the staker set AND in the directory for the hash.
    // Handles are cloned so the test can eject A between the two fetches.
    let mutable_staker = Arc::new(MutableStakerSet::new(HashSet::from([a_dht])));
    let mutable_dir = Arc::new(MutableOriginDirectory::new(HashMap::from([(
        U256::ZERO,
        vec![a_dht],
    )])));

    let mut addr_map = HashMap::new();
    addr_map.insert(a_dht, a_eth.address());
    let buyer = Arc::new(StubOpener {
        pool_id,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;

    // Provisioned inline (the shared builders hardcode `ConfigStakerSet` /
    // `StaticOriginDirectory`, which cannot be mutated mid-test). A default 15s
    // positive-cache TTL keeps the fetch #1 entry live through the ejection and
    // fetch #2, so only the `is_active` re-check can drop it.
    let (engine, _engine_tmp) = throwaway_engine().await?;
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(b_dht))),
        staker_set: Arc::clone(&mutable_staker) as Arc<dyn StakerSet>,
        origin_directory: Arc::clone(&mutable_dir) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: b_dht,
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(&local_rep),
        negative_cache: NegativeProbeCache::new(),
        probe_cache: PositiveProbeCache::new(),
        metrics: Arc::clone(&b_metrics),
        registry_regions: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        config: NodeOriginConfig {
            probe_fanout: 5,
            // Generous by intent, like [`DEFAULT_TEST_PULL_DEADLINES`]: a loaded
            // runner must not end a pull this fixture is not measuring.
            pull_timeout: DEFAULT_TEST_PULL_DEADLINES.0,
            stall_window: DEFAULT_TEST_PULL_DEADLINES.1,
            min_throughput_bps: 0,
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            // Reactive mid-pull top-up OFF (#1530): this fixture asserts what a pull
            // does when its channel runs dry, which a self-funding one would hide.
            working_deposit: U256::ZERO,
            seller_reserve: U256::ZERO,
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
            serve_economics: std::sync::Arc::new(decdn_node::serve_economics::OffPolicy),
            operator_shares: decdn_node::fee_shares::OperatorShares::new(6000),
            frequency_estimator: None,
            sell_rate_base: 0,
            // A budget far larger than any test's buy cost, so these fixtures warm
            // freely and the ADR 041 gate never changes their behaviour.
            warming: std::sync::Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
                1_000_000_000,
                0,
            )),
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        engine: engine.clone(),
    });

    // Fetch #1: cold path. `find_providers` is empty (no routing entries), so the
    // directory fallback returns [A]; A is probed once, cached, and delivers.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?;
    anyhow::ensure!(
        matches!(first, OriginFetch::AlreadyAdmitted)
            && engine.get(hash).await?.as_ref() == payload.as_slice(),
        "first fetch must deliver the blob from A"
    );
    anyhow::ensure!(
        a_probes.load(Ordering::SeqCst) == 1,
        "A must be probed exactly once on the cold path, got {}",
        a_probes.load(Ordering::SeqCst)
    );
    // One logical pull to A opens ONE connection: `pull_from_candidate`'s
    // whole-blob header handshake is also the drive's first and only leg, which
    // adopts it (#2063). The assertion pins the connection count, so it also
    // proves the handshake was not thrown away.
    anyhow::ensure!(
        a_streams.load(Ordering::SeqCst) == 1,
        "A must be pulled exactly once (the handshake the drive adopts) on the cold path, \
         got {}",
        a_streams.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_hits_total", 0)?;

    // Eject A inside the live TTL: a chain ejection drops it from BOTH the staker
    // set and the directory atomically. The probe-cache entry for [A] is untouched
    // and still live, so nothing but the hit-path `is_active` re-check can deny it.
    mutable_staker.remove(&a_dht);
    mutable_dir.remove(U256::ZERO);

    // Fetch #2: the entry is still live, so the hit path walks [A] — but A is no
    // longer active, so `cached_candidates` drops it, returns None, and the fetch
    // records a probe-cache MISS (a hit that serves no one is not a hit). The cold
    // fall-through then finds no providers (routing empty + directory now empty),
    // so the fetch is a clean NotFound. Crucially, A is never re-contacted.
    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?;
    anyhow::ensure!(
        matches!(second, OriginFetch::NotFound),
        "second fetch must miss — the only cached provider was ejected inside the TTL"
    );
    // Still 1 — the one connection from fetch #1's single pull. If the hit path
    // re-served the ejected A, this would climb.
    anyhow::ensure!(
        a_streams.load(Ordering::SeqCst) == 1,
        "the hit re-served an EJECTED A — `is_active` must deny it on the hit path, got {} \
         streams",
        a_streams.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        a_probes.load(Ordering::SeqCst) == 1,
        "the ejected A must not be re-probed, got {}",
        a_probes.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 0)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;
    assert_counter(&b_metrics, "node_pull_no_providers_total", 1)?;

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// #1530: reactive mid-pull deposit top-up on the node-to-node buyer leg.
//
// The daemon's miss pull opens a channel at the small INITIAL deposit and, until
// #1530, could only spend it: `stream_fetch_shared` at a fixed `byte_offset == 0`
// has no resume point, so a blob larger than that deposit simply failed. The pull
// now resumes at the PAID FRONTIER after funding the shortfall, which is what these
// tests pin — the completion, and, more importantly, that resuming costs the buyer
// nothing it had already bought.
// ---------------------------------------------------------------------------

/// [`support::honest_bao_wire_range`], visible under this suite's name.
fn honest_bao_wire_range(payload: &[u8], byte_offset: u64, byte_len: u64) -> Result<Vec<u8>> {
    support::honest_bao_wire_range(payload, byte_offset, byte_len)
}

fn honest_bao_wire_from(payload: &[u8], byte_offset: u64) -> Result<Vec<u8>> {
    support::honest_bao_wire_range(payload, byte_offset, 0)
}

/// The on-chain deposit an upstream can see, shared with the buyer's opener.
///
/// A real lane rejects a voucher whose cumulative `amount` exceeds the
/// signer's capability cap (`VoucherRejectReason::SpendingCapExhausted`), and a real
/// `topUp` raises the pool's escrowed deposit backing that cap. Modelling it
/// as one shared cell is what makes the round trip real here:
/// `FundingOpener`'s `top_up_pool_by` raises the same
/// number the server enforces, so the resumed leg succeeds for the RIGHT reason
/// rather than because the fixture stopped objecting.
type SharedDeposit = Arc<Mutex<U256>>;

fn shared_deposit(micro_usdc: u64) -> SharedDeposit {
    Arc::new(Mutex::new(U256::from(micro_usdc)))
}

fn read_deposit(cell: &SharedDeposit) -> Result<U256> {
    Ok(*cell
        .lock()
        .map_err(|_| anyhow::anyhow!("deposit lock poisoned"))?)
}

/// A buyer-channel opener that FUNDS, so the reactive leg has something to spend.
///
/// [`StubOpener`] with two differences that matter: its deposit is a live cell an
/// upstream fixture reads (see [`SharedDeposit`]), and `top_up_pool_by` adds to that
/// cell and logs the call. `funds` is what a test flips to model a top-up that
/// lands but adds no headroom.
#[derive(Debug)]
struct FundingOpener {
    pool_id: B256,
    /// What the BUYER believes it has escrowed — the `ctx.deposit` the pull's
    /// headroom arithmetic reads.
    deposit: SharedDeposit,
    /// What the UPSTREAM enforces. Normally the same number, raised in lockstep by a
    /// top-up. A test drives them APART to model an upstream lying about our deposit,
    /// which is the case `genuine_exhaustion` exists to refuse.
    ceiling: SharedDeposit,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    recorded: Arc<Mutex<Vec<ProgressEntry>>>,
    /// Every `top_up_pool_by(additional)` in order — the test's view of what the pull
    /// tried to fund.
    topups: Arc<Mutex<Vec<(Address, U256)>>>,
    /// Whether a top-up actually adds headroom. `false` models a refusal.
    funds: bool,
    /// The most one top-up lands. `None` lands the full request; `Some` models a
    /// short landing, such as a joined `topUp` that credited less than requested.
    landing_cap: Option<U256>,
}

#[async_trait]
impl PoolOpener for FundingOpener {
    async fn open_or_reuse_pool(
        &self,
        provider_addr: Address,
        _budget: Duration,
    ) -> Result<PoolContext> {
        let recorded = self
            .recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?;
        let (prior_bytes_delivered, prior_amount) = recorded
            .iter()
            .rev()
            .find(|(provider, ..)| *provider == provider_addr)
            .map_or((U256::ZERO, U256::ZERO), |(_, b, a)| (*b, *a));
        drop(recorded);
        Ok(PoolContext {
            pool_id: self.pool_id,
            provider: provider_addr,
            deposit: read_deposit(&self.deposit)?,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_bytes_delivered,
            prior_amount,
            client_binding: None,
            capability: None,
        })
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        pool_id: B256,
        write: decdn_client::buyer_pool::ProgressWrite,
    ) -> Result<()> {
        let totals = write.totals();
        let (bytes_delivered, amount) = (totals.last_bytes, totals.last_amount);
        anyhow::ensure!(
            pool_id == self.pool_id,
            "record_progress pool_id {pool_id} != opened pool {}",
            self.pool_id
        );
        self.recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?
            .push((provider_addr, bytes_delivered, amount));
        Ok(())
    }

    async fn top_up_pool_by(&self, additional: U256) -> Result<TopUpLanded> {
        self.topups
            .lock()
            .map_err(|_| anyhow::anyhow!("topups lock poisoned"))?
            .push((Address::ZERO, additional));
        let mut deposit = self
            .deposit
            .lock()
            .map_err(|_| anyhow::anyhow!("deposit lock poisoned"))?;
        // Add `additional`, capped at `landing_cap`, and report what landed, as the
        // real `BuyerPoolService::top_up_pool_by` does.
        let added = if self.funds {
            self.landing_cap
                .map_or(additional, |cap| additional.min(cap))
        } else {
            U256::ZERO
        };
        if !added.is_zero() {
            *deposit = deposit.saturating_add(added);
            // The escrow the upstream can see rises with it — a real `topUp` raises one
            // number, and the buyer's belief and the seller's gate are both views of it.
            *self
                .ceiling
                .lock()
                .map_err(|_| anyhow::anyhow!("ceiling lock poisoned"))? = *deposit;
        }
        Ok(TopUpLanded {
            new_deposit: *deposit,
            added,
        })
    }
}

/// Serve `payload` with real per-interval voucher pacing, honouring the request's
/// `byte_offset`, and REFUSE any voucher the shared deposit cannot cover.
///
/// The upstream half of the reactive-top-up round trip, and the only fixture in this
/// file that can produce a mid-blob `InsufficientDeposit`: `serve_then_reject_voucher`
/// refuses the CLOSING voucher over a raw payload, so nothing is ever delivered and
/// resumed. Here the buyer is paid for what it received, told it cannot afford the
/// next interval, and — once the deposit rises — served the remainder from wherever
/// it asks to resume.
#[allow(clippy::too_many_arguments)]
async fn serve_with_deposit_ceiling(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    payloads: &[Arc<Vec<u8>>],
    rate: u64,
    deposit: &SharedDeposit,
    resume_delay: Duration,
    initial_ceiling: U256,
    lane: &Mutex<ScriptedLane>,
) -> Result<()> {
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| anyhow::anyhow!("accept_bi: {e}"))?;
    let req = read_stream_request(&mut recv).await?;
    // Route on the requested hash, so one server can hold several blobs — the
    // concurrent-pulls regression test needs two pulls on ONE channel.
    let payload: &[u8] = payloads
        .iter()
        .find(|p| *Hash::new(p.as_slice()).as_bytes() == req.hash)
        .map(|p| p.as_slice())
        .ok_or_else(|| anyhow::anyhow!("deposit-capped upstream: unknown hash requested"))?;
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let (resp, resp_ext) = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(&mut send, &encode_stream_response(&resp, Some(&resp_ext))?)
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;

    // Serve the completing POST-TOP-UP leg slowly, AFTER the response is on the wire
    // so the delay lands in the chunk stream (`pull_to_sink`), not the open. The
    // paid-wait accounting must count this as the upstream serving bytes, not as our
    // funding wait — it must not be charged against the delivery-speed elapsed clock
    // (#1602). Zero for every test but the delivery-speed regression.
    //
    // The completing leg is the one served once a top-up has raised the shared escrow
    // above its initial ceiling (`top_up_pool_by` raises `deposit` here). Keying on the
    // raised ceiling — not on `req.byte_offset` — is deliberate: the buyer re-requests
    // the whole blob at `byte_offset == 0` and lets its ranged store dedupe the prefix,
    // so a `byte_offset > 0` gate never fires on this path and the throttle would be
    // inert.
    // `resume_delay.is_zero()` first so the common (un-throttled) case short-circuits
    // before taking the ceiling lock.
    if !resume_delay.is_zero() && read_deposit(deposit)? > initial_ceiling {
        tokio::time::sleep(resume_delay).await;
    }

    let wire = honest_bao_wire_from(payload, req.byte_offset)?;
    let mut unvouchered: u64 = 0;
    for chunk in wire.chunks(WIRE_FRAME) {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
        unvouchered = unvouchered.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if unvouchered >= CHUNK_BYTES {
            if !settle_voucher(&mut send, &mut recv, deposit, lane).await? {
                // Refused: hold the connection so the buyer reads the rejection frame
                // rather than a transport reset (which would score as unreachable).
                let _ = send.finish();
                conn.closed().await;
                return Ok(());
            }
            unvouchered = 0;
        }
    }
    if unvouchered > 0 && !settle_voucher(&mut send, &mut recv, deposit, lane).await? {
        let _ = send.finish();
        conn.closed().await;
        return Ok(());
    }
    write_frame(&mut send, &encode_message(&ClientMessage::StreamEnd)?)
        .await
        .map_err(|e| anyhow::anyhow!("write end: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
}

/// Read the buyer's voucher and either accept it or refuse it `SpendingCapExhausted`,
/// exactly as the `PaymentPool` would: the voucher's CUMULATIVE amount is what the
/// deposit has to cover. Returns whether it was accepted.
///
/// `bundle: None`, which is what a real node attaches when it holds no prior accepted
/// voucher to echo — and, critically, what keeps `genuine_exhaustion`'s desync
/// carve-out out of the way. A bundle that ADVANCED our watermark would (correctly)
/// route to the reseed path instead of to funding.
///
/// Acceptance is implicit (continued delivery is the ack, ADR 005), so on accept no
/// reply is written; only a rejection sends a message.
#[expect(
    clippy::print_stderr,
    reason = "test harness diagnostic surfaced in the nextest log"
)]
async fn settle_voucher(
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
    deposit: &SharedDeposit,
    lane: &Mutex<ScriptedLane>,
) -> Result<bool> {
    // Read proofs until one credits this chunk. The payer sends housekeeping
    // vouchers ahead of a reveal (a stream's first root voucher for an epoch, a
    // rollover), and neither advances the claim.
    for _ in 0..4u8 {
        let proof = read_proof(recv).await?;
        // Evaluate against a COPY first. A rejected proof must leave the lane
        // exactly as it found it — the real node stages a candidate and discards it
        // on refusal (`stage_voucher_rejects_without_advancing_candidate`), and a
        // fixture that kept the amount of a voucher it just refused would credit
        // the payer with money the upstream declined to be paid.
        let (advanced, claim, candidate) = {
            let lane = lane
                .lock()
                .map_err(|_| anyhow::anyhow!("scripted lane lock poisoned"))?;
            let mut candidate = *lane;
            let advanced = candidate.apply(&proof);
            (advanced, candidate.claim(), candidate)
        };
        eprintln!(
            "DIAG proof={} adv={advanced} claim={claim} dep={}",
            match &proof {
                ClientMessage::Voucher(v) =>
                    format!("V(amt={},root={:02x})", v.amount, v.chain_root[0]),
                ClientMessage::ChunkPreimage(p) => format!("P(idx={})", p.index),
                _ => "?".to_string(),
            },
            read_deposit(deposit)?
        );
        // The deposit has to cover the CHAIN-EXTENDED claim, not just the signed
        // anchor — a reveal spends real money without a signature.
        if claim > read_deposit(deposit)? {
            write_frame(
                send,
                &encode_message(&ClientMessage::StreamError(StreamError::VoucherRejected {
                    reason: VoucherRejectReason::SpendingCapExhausted,
                    bundle: None,
                }))?,
            )
            .await
            .map_err(|e| anyhow::anyhow!("write rejection: {e}"))?;
            return Ok(false);
        }
        *lane
            .lock()
            .map_err(|_| anyhow::anyhow!("scripted lane lock poisoned"))? = candidate;
        if advanced {
            return Ok(true);
        }
    }
    anyhow::bail!("deposit-capped upstream: four proofs in a row credited nothing")
}

/// Spawn a provider that answers probes truthfully and serves under a live deposit
/// ceiling (see [`serve_with_deposit_ceiling`]).
#[allow(clippy::too_many_arguments)]
fn spawn_deposit_capped_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    payloads: Vec<Arc<Vec<u8>>>,
    rate: u64,
    deposit: SharedDeposit,
    resume_delay: Duration,
    initial_ceiling: U256,
) -> tokio::task::JoinHandle<()> {
    // Same-length blobs only, so the probe's single quoted `total_bytes` is honest
    // for all of them (`two_concurrent_pulls_...` makes the same choice for the
    // same reason).
    let total_bytes = payloads
        .first()
        .map_or(0, |p| u64::try_from(p.len()).unwrap_or(u64::MAX));
    let payloads = Arc::new(payloads);
    // ONE lane, shared by every stream this upstream serves — which is what a
    // lane is: `(pool_id, signer, provider)`, not `(…, stream)`. Concurrent
    // pulls to one provider share a chain and a claim, so a per-stream view
    // would let each one spend the whole deposit independently.
    let lane: Arc<Mutex<ScriptedLane>> = Arc::new(Mutex::new(ScriptedLane::default()));
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let eth = Arc::clone(&a_eth);
            let dom = slash.clone();
            let payloads = Arc::clone(&payloads);
            let deposit = Arc::clone(&deposit);
            let lane = Arc::clone(&lane);
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ = serve_with_deposit_ceiling(
                        conn,
                        &eth,
                        &dom,
                        &payloads,
                        rate,
                        &deposit,
                        resume_delay,
                        initial_ceiling,
                        &lane,
                    )
                    .await;
                });
            }
        }
    })
}

/// Everything a reactive-top-up test needs to drive one fetch and then inspect what
/// it paid.
struct TopUpFixture {
    origin: NodeOrigin,
    /// The engine the origin streams pulled bytes into — a test reads them back with
    /// `engine.get(hash)` after a fetch reports `AlreadyAdmitted`.
    engine: CacheEngine,
    /// The engine's backing directory. Held for the fixture's lifetime so its drop does
    /// not reclaim the store out from under an in-flight or still-inspected engine.
    _engine_tmp: tempfile::TempDir,
    opener: Arc<FundingOpener>,
    metrics: Arc<Metrics>,
    local_rep: Arc<LocalReputation>,
    provider: iroh::PublicKey,
    recorded: Arc<Mutex<Vec<ProgressEntry>>>,
    ep_a: iroh::Endpoint,
    ep_b: iroh::Endpoint,
    task_a: tokio::task::JoinHandle<()>,
}

impl TopUpFixture {
    async fn shutdown(self) -> Result<()> {
        shutdown([self.task_a], [&self.ep_b, &self.ep_a]).await?;
        Ok(())
    }
}

/// How one reactive-top-up scenario is wired.
#[derive(Debug, Clone, Copy)]
struct TopUpSetup {
    /// What the buyer's channel starts with.
    initial_micro_usdc: u64,
    /// The graduation target; `0` disables the reactive leg.
    working_micro_usdc: u64,
    /// Whether a top-up adds headroom.
    funds: bool,
    /// The most one top-up lands, if it must land short. `None` lands the request.
    landing_cap_micro_usdc: Option<u64>,
    /// What the UPSTREAM enforces, if it must differ from what the buyer believes.
    /// `None` keeps them equal, which is the honest case.
    ceiling_micro_usdc: Option<u64>,
    /// How slowly the upstream serves the RESUME leg, injected into the chunk stream
    /// after the open. `ZERO` for every scenario but the delivery-speed regression,
    /// which needs a completing post-top-up leg whose transfer time is measurable.
    resume_delay: Duration,
}

impl TopUpSetup {
    /// An honest upstream: its ceiling is the buyer's deposit, and a top-up raises both.
    const fn honest(initial_micro_usdc: u64, working_micro_usdc: u64, funds: bool) -> Self {
        Self {
            initial_micro_usdc,
            working_micro_usdc,
            funds,
            landing_cap_micro_usdc: None,
            ceiling_micro_usdc: None,
            resume_delay: Duration::ZERO,
        }
    }
}

/// Stand up one deposit-capped upstream and a buyer whose channel starts at
/// `setup.initial_micro_usdc` and graduates to `setup.working_micro_usdc`.
async fn top_up_fixture(payload: Arc<Vec<u8>>, setup: TopUpSetup) -> Result<TopUpFixture> {
    top_up_fixture_multi(vec![payload], setup).await
}

/// [`top_up_fixture`] over several SAME-LENGTH blobs on one provider (and thus one
/// channel), for the concurrent-pulls regression test.
async fn top_up_fixture_multi(
    payloads: Vec<Arc<Vec<u8>>>,
    setup: TopUpSetup,
) -> Result<TopUpFixture> {
    top_up_fixture_multi_rep(payloads, setup, LocalReputationConfig::default()).await
}

/// [`top_up_fixture_multi`] with the buyer's local reputation configured explicitly,
/// so the delivery-speed regression can read a single delivery's speed off the score
/// (`alpha = 1.0`, no EWMA blend) against a known `reference_bps`.
async fn top_up_fixture_multi_rep(
    payloads: Vec<Arc<Vec<u8>>>,
    setup: TopUpSetup,
    rep_config: LocalReputationConfig,
) -> Result<TopUpFixture> {
    let payload = Arc::clone(
        payloads
            .first()
            .ok_or_else(|| anyhow::anyhow!("at least one payload"))?,
    );
    let hash = Hash::new(payload.as_ref());
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let deposit = shared_deposit(setup.initial_micro_usdc);
    let ceiling = shared_deposit(setup.ceiling_micro_usdc.unwrap_or(setup.initial_micro_usdc));

    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_deposit_capped_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payloads,
        RATE,
        Arc::clone(&ceiling),
        setup.resume_delay,
        U256::from(setup.ceiling_micro_usdc.unwrap_or(setup.initial_micro_usdc)),
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(rep_config)?);
    let metrics = Arc::new(Metrics::new());
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let opener = Arc::new(FundingOpener {
        pool_id: B256::repeat_byte(0x15),
        deposit,
        ceiling,
        signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        topups: Arc::new(Mutex::new(Vec::new())),
        funds: setup.funds,
        landing_cap: setup.landing_cap_micro_usdc.map(U256::from),
    });
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, engine, engine_tmp) = build_origin_with_probe_caches(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        Arc::clone(&opener) as Arc<dyn PoolOpener>,
        &local_rep,
        &metrics,
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES.0,
        DEFAULT_TEST_PULL_DEADLINES.1,
        total_bytes.saturating_mul(4),
        NegativeProbeCache::new(),
        PositiveProbeCache::new(),
        U256::ZERO,
        U256::from(setup.working_micro_usdc),
    )
    .await;

    Ok(TopUpFixture {
        origin,
        engine,
        _engine_tmp: engine_tmp,
        opener,
        metrics,
        local_rep,
        provider: a_id,
        recorded,
        ep_a,
        ep_b,
        task_a,
    })
}

fn topup_log(opener: &FundingOpener) -> Result<Vec<(Address, U256)>> {
    Ok(opener
        .topups
        .lock()
        .map_err(|_| anyhow::anyhow!("topups lock poisoned"))?
        .clone())
}

/// A blob that costs more than the initial deposit but less than the working one:
/// three chunks of content, so the pull is refused mid-blob with real
/// delivered bytes behind it rather than at the very first voucher.
fn multi_interval_payload() -> Arc<Vec<u8>> {
    let len = usize::try_from(MB_BYTES).unwrap_or(usize::MAX) * 3 + 777;
    Arc::new(
        (0..len)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect(),
    )
}

/// The headline case: a single node→node pull larger than the channel's working
/// deposit now completes, by funding the shortfall and resuming — the whole point
/// of #1530.
///
/// Before this, the pull ended at the first voucher the working deposit could not
/// cover, and no retry could help: every candidate opens at the same working
/// deposit, and a from-zero retry would re-spend the fresh deposit on bytes
/// it had already bought and re-exhaust at the same offset.
#[tokio::test(flavor = "multi_thread")]
async fn a_pull_larger_than_the_working_deposit_tops_up_once_and_completes() -> Result<()> {
    let payload = multi_interval_payload();
    // One interval's worth of headroom at RATE — enough to be paid for real
    // delivered bytes, nowhere near enough for the whole blob.
    let fixture = top_up_fixture(
        Arc::clone(&payload),
        TopUpSetup::honest(2 * RATE, 200 * RATE, true),
    )
    .await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the reactive top-up pull never finished"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;

    anyhow::ensure!(
        matches!(got, OriginFetch::AlreadyAdmitted),
        "a topped-up pull must deliver the blob, not NotFound"
    );
    let bytes = fixture.engine.get(Hash::new(payload.as_ref())).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "the resumed pull delivered {} bytes, expected {}",
        bytes.len(),
        payload.len()
    );

    let log = topup_log(&fixture.opener)?;
    anyhow::ensure!(
        log.len() == 1,
        "exactly one reactive top-up should have funded this pull, got {log:?}"
    );
    // The top-up restores spendable headroom to the WORKING deposit: it adds the
    // working deposit less what the pool still had, and the pool never had more
    // than its initial deposit.
    anyhow::ensure!(
        log.first().is_some_and(|(_, additional)| {
            *additional >= U256::from(200 * RATE - 2 * RATE)
                && *additional <= U256::from(200 * RATE)
        }),
        "the top-up must raise spendable to the WORKING deposit, got {log:?}"
    );
    assert_counter(&fixture.metrics, "node_pull_reactive_topup_total", 1)?;
    assert_counter(
        &fixture.metrics,
        "node_pull_reactive_topup_refused_total",
        0,
    )?;
    // The upstream did nothing wrong — it served every byte it was paid for.
    assert_counter(&fixture.metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&fixture.metrics, "node_pull_corruption_total", 0)?;
    anyhow::ensure!(
        fixture.local_rep.score(fixture.provider) > 0.5,
        "an upstream that served through a top-up must be scored as delivering, got {}",
        fixture.local_rep.score(fixture.provider)
    );

    fixture.shutdown().await?;
    Ok(())
}

/// The completing post-top-up leg is REAL delivery and its transfer time must reach
/// the delivery-speed reputation signal (ADR 008) — a slow upstream that served
/// through a top-up must score slow, not instantaneous (#1602).
///
/// `paid_wait` excludes chain settlement and the re-open probes from `elapsed`, but it
/// must NOT excuse the streaming of the resumed leg. The bug timed the whole completing
/// `stream_leg` — open AND `pull_to_sink` — because `note_successful_open` clears
/// `awaiting_topup_settle` between them, so the flag read at the loop top still said
/// "charge this leg". The entire remainder's transfer then landed in `paid_wait` and
/// was subtracted out of `elapsed`, scoring the upstream as if it delivered in zero
/// time.
///
/// # Why this pins what `score(provider) > 0.5` cannot
///
/// The reputation is configured so ONE delivery's speed reads straight off the score:
/// `alpha = 1.0` drops the EWMA blend, so `score == 0.4·speed + 0.6` with the
/// log-normalized `speed = clamp(ln(1+bytes_per_sec) / ln(1+reference_bps), 0, 1)`
/// (ADR 008 §Local Score Calculation). The upstream is throttled to serve the resume
/// leg over [`SLOW_RESUME`], so the honest `elapsed` spans at least that:
///
/// - CORRECT: the whole ~3,146,505-byte payload crosses over the throttled
///   `SLOW_RESUME = 3 s`, so `bytes_per_sec ≈ 1,048,835` (~1 MiB/s). At
///   `reference_bps = 1 GiB/s` (`1,073,741,824`, chosen — as the log curve
///   recommends — well above the throttled rate so the log gap is legible), that is
///   `speed = ln(1,048,836) / ln(1,073,741,825) ≈ 13.86319 / 20.79442 ≈ 0.66668`, so
///   `score ≈ 0.4·0.66668 + 0.6 ≈ 0.86667` — comfortably under the bound below.
/// - BUGGED: the ~3 s transfer is folded into `paid_wait`, leaving `elapsed ≈ the
///   pre-top-up leg` (sub-second, no throttle). Even a lenient 0.1 s bug-elapsed
///   already yields `bytes_per_sec ≈ 31,465,050`, `speed ≈ 0.83`, `score ≈ 0.932`; a
///   realistic sub-10ms elapsed pins `speed` near 1 and `score` near `1.0`. Either
///   way the bugged score clears the bound below by a wide margin.
///
/// `> 0.5` — what the other top-up tests assert — passes both, which is exactly why it
/// never caught this.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_post_topup_delivery_scores_slow_not_instant() -> Result<()> {
    /// Long enough that the resumed leg's transfer dominates `elapsed`, so a score that
    /// still reads "fast" can only mean the transfer was wrongly charged to `paid_wait`.
    const SLOW_RESUME: Duration = Duration::from_secs(3);
    /// 1 GiB/s — the production default reference (`DEFAULT_REFERENCE_BPS`), set
    /// explicitly here for a self-contained derivation. Picked well above the
    /// throttled resume's ~1 MiB/s so the log curve's gap between "throttled" and
    /// "saturated" reads clearly (see the derivation above).
    const REFERENCE_BPS: u64 = 1024 * 1024 * 1024;

    let payload = multi_interval_payload();
    let mut rep_config = LocalReputationConfig::default();
    // One delivery must move the score to exactly its interaction sample, so the speed
    // term is legible; the default 0.1 EWMA would compress both cases against neutral.
    rep_config.alpha = 1.0;
    rep_config.reference_bps = REFERENCE_BPS;

    let mut setup = TopUpSetup::honest(2 * RATE, 200 * RATE, true);
    setup.resume_delay = SLOW_RESUME;
    let fixture = top_up_fixture_multi_rep(vec![Arc::clone(&payload)], setup, rep_config).await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the reactive top-up pull never finished"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::AlreadyAdmitted),
        "a topped-up pull must deliver the blob, not NotFound"
    );
    let bytes = fixture.engine.get(Hash::new(payload.as_ref())).await?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "the resumed pull delivered {} bytes, expected {}",
        bytes.len(),
        payload.len()
    );

    // The pull DID top up and complete — the same path the headline test drives.
    assert_counter(&fixture.metrics, "node_pull_reactive_topup_total", 1)?;
    assert_counter(&fixture.metrics, "node_pull_corruption_total", 0)?;
    assert_counter(&fixture.metrics, "node_pull_unreachable_total", 0)?;

    // The delivery scored, so the speed term is live (`score > 0.6` means `speed > 0`).
    // The bound that matters: the throttled resume gives an HONEST `elapsed` a
    // log-curve speed of ≈0.66668, i.e. `score ≈ 0.86667` (derived above). The bug
    // charges the transfer to `paid_wait`, leaving a sub-second `elapsed` whose
    // log-curve speed is ≥0.83 even under a lenient 0.1 s assumption (score ≥0.93),
    // and near 1.0 for a realistic sub-10ms elapsed. 0.90 sits with clear margin
    // above the honest score and below every bugged one.
    let score = fixture.local_rep.score(fixture.provider);
    anyhow::ensure!(
        score > 0.6,
        "a completed delivery must credit correctness+reachability, got {score}"
    );
    anyhow::ensure!(
        score < 0.90,
        "a slow post-top-up delivery must score slow; a score of {score} means the resume \
         leg's transfer was wrongly excluded from `elapsed` (folded into `paid_wait`) — #1602"
    );

    fixture.shutdown().await?;
    Ok(())
}

/// The money assertion: resuming after a top-up must not re-buy the prefix.
///
/// The persisted watermark is channel-cumulative WIRE bytes, so the whole pull's
/// spend is directly comparable to what ONE bao encoding of the blob costs. A naive
/// `byte_offset = 0` restart — the thing that made the reactive leg impossible on
/// this path — bills roughly twice that, and fails the upper bound loudly.
///
/// The bounds are asymmetric on purpose:
///
/// - the LOWER bound is the wire cost of the whole blob. Falling under it means
///   content was delivered that no voucher covered — an under-pay, free bandwidth
///   taken from the upstream. Computed in WIRE bytes, not content: a content-sized
///   floor would pass while the bao proof overhead went unbilled, which is exactly
///   the mistake `content_paid_frontier` exists to prevent.
/// - the UPPER bound allows the resumed leg's re-sent root->offset proof path plus
///   strictly under one chunk group of re-fetched content — the bounded, deliberate
///   over-pay of resuming at a group boundary. It does NOT allow a second copy of
///   the blob.
#[tokio::test(flavor = "multi_thread")]
async fn the_resumed_leg_does_not_re_pay_for_delivered_bytes() -> Result<()> {
    let payload = multi_interval_payload();
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let fixture = top_up_fixture(
        Arc::clone(&payload),
        TopUpSetup::honest(2 * RATE, 200 * RATE, true),
    )
    .await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the reactive top-up pull never finished"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::AlreadyAdmitted),
        "the pull must have completed for its spend to mean anything"
    );

    let log = progress_log(&fixture.recorded)?;
    let (_, billed_wire, _) = log
        .last()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("a paid pull must have persisted a watermark"))?;
    let billed_wire = u64::try_from(billed_wire).unwrap_or(u64::MAX);

    let whole_blob_wire = u64::try_from(honest_bao_wire_from(&payload, 0)?.len()).unwrap_or(0);
    // The resumed leg re-sends a proof path from the root down to its offset. Bounded
    // by the whole tree's proof overhead, which is `wire - content`.
    let proof_slack = whole_blob_wire.saturating_sub(total_bytes);
    let ceiling = whole_blob_wire
        .saturating_add(proof_slack)
        .saturating_add(decdn_cache::CHUNK_GROUP_BYTES);

    anyhow::ensure!(
        billed_wire >= whole_blob_wire,
        "UNDER-PAY: billed {billed_wire} wire bytes for a blob whose single encoding costs \
         {whole_blob_wire}. Content was delivered that no voucher covered — resuming past the \
         PAID frontier instead of at it"
    );
    anyhow::ensure!(
        billed_wire <= ceiling,
        "DOUBLE-PAY: billed {billed_wire} wire bytes, but one encoding costs {whole_blob_wire} \
         and the resume slack allows at most {ceiling}. The resumed leg re-bought the prefix — \
         which is what resuming from zero does"
    );

    fixture.shutdown().await?;
    Ok(())
}

/// An upstream claiming `InsufficientDeposit` while OUR ledger still covers the next
/// voucher is lying or broken, and must not be funded.
///
/// This is the whole reason `genuine_exhaustion` validates the claim against the
/// buyer's own accounting rather than taking the wire word for it: without it, any
/// upstream could make a node escrow more USDC on demand, repeatedly, by refusing
/// vouchers it could perfectly well accept.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_crying_poverty_while_our_ledger_has_headroom_is_not_funded() -> Result<()> {
    let payload = multi_interval_payload();
    // The buyer's channel is funded far beyond the whole blob, so nothing the upstream
    // can say about our deposit is true — but its ceiling is set to refuse the very
    // first voucher anyway. That divergence IS the attack: an upstream that can make a
    // node escrow more USDC just by refusing vouchers it could accept.
    let fixture = top_up_fixture(
        Arc::clone(&payload),
        TopUpSetup {
            initial_micro_usdc: 10_000 * RATE,
            working_micro_usdc: 20_000 * RATE,
            funds: true,
            landing_cap_micro_usdc: None,
            ceiling_micro_usdc: Some(0),
            resume_delay: Duration::ZERO,
        },
    )
    .await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the pull never ended"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a refused pull must not surface bytes"
    );

    anyhow::ensure!(
        topup_log(&fixture.opener)?.is_empty(),
        "we must NOT escrow USDC on an upstream's unsupported word"
    );
    assert_counter(&fixture.metrics, "node_pull_reactive_topup_total", 0)?;
    // ...and the refusal is VISIBLE. This is the counter's whole reason to exist —
    // it is the only signal that distinguishes a peer extorting escrow from an
    // ordinary failed pull, and until #1600 the case could never reach it.
    assert_counter(
        &fixture.metrics,
        "node_pull_reactive_topup_refused_total",
        1,
    )?;

    fixture.shutdown().await?;
    Ok(())
}

/// A top-up that lands but adds no headroom ends the pull instead of looping.
///
/// Retrying on an unchanged deposit exhausts at exactly the same offset, so the loop
/// would spin against a wall while a client waits.
#[tokio::test(flavor = "multi_thread")]
async fn a_topup_that_adds_no_headroom_ends_the_pull() -> Result<()> {
    let payload = multi_interval_payload();
    let fixture = top_up_fixture(
        Arc::clone(&payload),
        TopUpSetup::honest(2 * RATE, 200 * RATE, false),
    )
    .await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("an unfundable pull must not hang"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "an unfundable pull must not surface bytes"
    );

    let log = topup_log(&fixture.opener)?;
    anyhow::ensure!(
        log.len() == 1,
        "the funding attempt must be made exactly once, then abandoned — got {log:?}"
    );
    assert_counter(&fixture.metrics, "node_pull_reactive_topup_total", 0)?;
    assert_counter(
        &fixture.metrics,
        "node_pull_reactive_topup_refused_total",
        1,
    )?;

    fixture.shutdown().await?;
    Ok(())
}

/// A top-up that lands SHORT of its request (#2012): the pull keeps the headroom that
/// landed and spends it, uses its single reactive top-up, and is metered once as
/// refused — not as a success, and not a second time as an extortion refusal.
///
/// The initial deposit covers two MiB at `RATE` and the capped landing one more,
/// short of the 3 MiB + 777 byte blob, so the pull ends without the blob.
#[tokio::test(flavor = "multi_thread")]
async fn a_topup_that_lands_short_keeps_what_landed_and_is_metered_once() -> Result<()> {
    let payload = multi_interval_payload();
    let initial = 2 * RATE;
    let cap = RATE;
    let fixture = top_up_fixture(
        Arc::clone(&payload),
        TopUpSetup {
            landing_cap_micro_usdc: Some(cap),
            ..TopUpSetup::honest(initial, 200 * RATE, true)
        },
    )
    .await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("an underfunded pull must not hang"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a pull the short landing cannot finish must not surface bytes"
    );

    let log = topup_log(&fixture.opener)?;
    anyhow::ensure!(
        log.len() == 1,
        "the single reactive top-up must be spent exactly once — got {log:?}"
    );
    anyhow::ensure!(
        read_deposit(&fixture.opener.deposit)? == U256::from(initial + cap),
        "the short landing must raise the deposit by exactly what landed"
    );
    let paid = progress_log(&fixture.recorded)?
        .iter()
        .map(|(_, _, amount)| *amount)
        .max()
        .unwrap_or(U256::ZERO);
    anyhow::ensure!(
        paid > U256::from(initial),
        "the pull must spend the headroom that landed: paid {paid}, initial deposit {initial}"
    );
    assert_counter(&fixture.metrics, "node_pull_reactive_topup_total", 0)?;
    assert_counter(
        &fixture.metrics,
        "node_pull_reactive_topup_refused_total",
        1,
    )?;

    fixture.shutdown().await?;
    Ok(())
}

/// `buyer_working_deposit_micro_usdc = 0` disables the reactive leg outright, the
/// same sentinel the proactive low-water refill honours. An operator who turns
/// top-ups off must not have one performed on their behalf.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_working_deposit_never_funds_a_pull() -> Result<()> {
    let payload = multi_interval_payload();
    let fixture =
        top_up_fixture(Arc::clone(&payload), TopUpSetup::honest(2 * RATE, 0, true)).await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the pull never ended"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "with top-up disabled an exhausted pull must fail, not fund itself"
    );
    anyhow::ensure!(
        topup_log(&fixture.opener)?.is_empty(),
        "a zero working deposit must not escrow anything"
    );

    fixture.shutdown().await?;
    Ok(())
}

/// Two CONCURRENT pulls on one channel, one of them resuming across a top-up: the
/// resume frontier must never overshoot the bytes that pull actually decoded
/// (#1600 review).
///
/// The frontier is derived from the CHANNEL's cumulative wire watermark, and the
/// ledger is shared with every concurrent pull on the channel (`BuyerLedgers`) — so
/// the other pull's acked vouchers inflate the delta. Uncapped, the inflated
/// frontier maps PAST the exhausted pull's decoded bytes; its `truncate` is then a
/// no-op (truncate never grows) and the resumed leg splices at the wrong position,
/// producing a blob that fails the cache engine's hash check — after the upstream
/// was already scored `Delivered`. The clamp in `resume_frontier` bounds the
/// frontier at the chunk-group floor of what the leg holds, so the worst case is a
/// bounded re-fetch of already-paid content and both blobs assemble exactly.
///
/// Concurrency makes the interleaving nondeterministic, so this cannot pin WHICH
/// pull exhausts first — but with the clamp both orderings succeed, and without it
/// the overshoot ordering corrupts, which is what a regression run trips on. This is
/// the deterministic pin of the `delivered_frontier` clamp in
/// `decdn_client::driver::drive`.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pulls_resume_at_their_own_frontier_not_the_channels() -> Result<()> {
    // Four chunks of content per blob: large enough that each stream's ramped
    // credit window (floored at one chunk) cannot front the whole payload on
    // credit, so real proofs come due mid-pull.
    let len = usize::try_from(CHUNK_BYTES).unwrap_or(usize::MAX) * 4 + 777;
    let payload_a: Arc<Vec<u8>> = Arc::new(
        (0..len)
            .map(|i| u8::try_from(i % 249).unwrap_or(0))
            .collect(),
    );
    let payload_b: Arc<Vec<u8>> = Arc::new(
        (0..len)
            .map(|i| u8::try_from(i % 247).unwrap_or(1))
            .collect(),
    );
    let hash_a = Hash::new(payload_a.as_ref());
    let hash_b = Hash::new(payload_b.as_ref());
    anyhow::ensure!(hash_a != hash_b, "fixtures must be distinct blobs");

    // The initial deposit funds roughly six wire intervals — enough for real
    // progress on both streams, but well short of the combined ~eight intervals the
    // two 4-interval blobs cost together, so the combined spend of the two
    // concurrent pulls exhausts it mid-blob, with real delivered-and-paid bytes on
    // BOTH streams behind the rejection. The working deposit is large enough for
    // both to finish afterward.
    let fixture = top_up_fixture_multi(
        vec![Arc::clone(&payload_a), Arc::clone(&payload_b)],
        TopUpSetup::honest(6 * RATE, 400 * RATE, true),
    )
    .await?;

    let (got_a, got_b) = tokio::time::timeout(Duration::from_mins(2), async {
        tokio::join!(
            Origin::fetch(&fixture.origin, hash_a, u64::MAX),
            Origin::fetch(&fixture.origin, hash_b, u64::MAX),
        )
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "concurrent top-up pulls never finished within 2m — likely stalled frontier or \
             wedge; topups={:?} pending={} stalled={} timeout={}",
            topup_log(&fixture.opener).unwrap_or_default(),
            counter_value(&fixture.metrics, "node_pull_pool_open_pending_total").unwrap_or(0),
            counter_value(&fixture.metrics, "node_pull_stalled_total").unwrap_or(0),
            counter_value(&fixture.metrics, "node_pull_timeout_total").unwrap_or(0)
        )
    })?;

    let got_a = got_a.map_err(|e| anyhow::anyhow!("fetch A: {e}"))?;
    anyhow::ensure!(
        matches!(&got_a, OriginFetch::AlreadyAdmitted),
        "pull A returned {got_a:?} (expected AlreadyAdmitted); a frontier overshoot past the \
         delivered bytes splices a gap and surfaces NotFound after the hash check, while a \
         stalled/timeout budget expiry leaves topup_log empty — got topups={:?} stalled={} \
         pending={}",
        topup_log(&fixture.opener).unwrap_or_default(),
        counter_value(&fixture.metrics, "node_pull_stalled_total").unwrap_or(0),
        counter_value(&fixture.metrics, "node_pull_pool_open_pending_total").unwrap_or(0)
    );
    let bytes_a = fixture.engine.get(hash_a).await?;
    let got_b = got_b.map_err(|e| anyhow::anyhow!("fetch B: {e}"))?;
    anyhow::ensure!(
        matches!(&got_b, OriginFetch::AlreadyAdmitted),
        "pull B returned {got_b:?} (expected AlreadyAdmitted); a frontier overshoot past the \
         delivered bytes splices a gap and surfaces NotFound after the hash check, while a \
         stalled/timeout budget expiry leaves topup_log empty — got topups={:?} stalled={} \
         pending={}",
        topup_log(&fixture.opener).unwrap_or_default(),
        counter_value(&fixture.metrics, "node_pull_stalled_total").unwrap_or(0),
        counter_value(&fixture.metrics, "node_pull_pool_open_pending_total").unwrap_or(0)
    );
    let bytes_b = fixture.engine.get(hash_b).await?;
    // EXACT bytes, which is the whole point: an overshot frontier assembles a blob
    // with a gap where the truncate could not rewind, and the engine's hash check
    // turns that into a NotFound (caught above) — never silently wrong bytes.
    anyhow::ensure!(bytes_a.as_ref() == payload_a.as_slice(), "pull A corrupted");
    anyhow::ensure!(bytes_b.as_ref() == payload_b.as_slice(), "pull B corrupted");

    // At least one pull must actually have crossed the ceiling and funded — else
    // the deposit sizing degenerated and this test stopped exercising the resume.
    anyhow::ensure!(
        !topup_log(&fixture.opener)?.is_empty(),
        "the initial deposit was never exhausted; the test no longer covers the resume path"
    );

    fixture.shutdown().await?;
    Ok(())
}

/// The reactive top-up is bounded at ONE per pull, over the wire (#1600 review).
///
/// The pure policy — refusing once `topups_used == max_topups` — is pinned in
/// `decdn_client::pacer`'s own unit tests; this pins the WIRING — the
/// `topups_used` increment [`decdn_client::driver::drive`] performs after a
/// landed top-up. Removing that increment is currently invisible to every other
/// test, because the funding double is idempotent against its target (a second call
/// at the same target adds nothing, so the funder reports no headroom and the loop
/// stops anyway).
/// Here the working deposit is deliberately still too small for the blob, so a loop
/// that did not count would keep funding-and-failing rather than ending after one.
#[tokio::test(flavor = "multi_thread")]
async fn a_working_deposit_that_still_cannot_cover_the_blob_funds_exactly_once() -> Result<()> {
    // ~10 chunks of wire, so the whole blob costs ~10x RATE. The initial
    // deposit funds two, and the top-up restores headroom to three more — enough for
    // real progress on the resumed leg, and still far short of the blob.
    let len = usize::try_from(MB_BYTES).unwrap_or(usize::MAX) * 9 + 777;
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..len)
            .map(|i| u8::try_from(i % 253).unwrap_or(0))
            .collect(),
    );
    let fixture = top_up_fixture(
        Arc::clone(&payload),
        TopUpSetup::honest(2 * RATE, 3 * RATE, true),
    )
    .await?;

    let got = tokio::time::timeout(
        Duration::from_mins(1),
        Origin::fetch(&fixture.origin, Hash::new(payload.as_ref()), u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a still-underfunded pull must not loop forever"))?
    .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "a pull the working deposit cannot cover must end as a miss"
    );

    let log = topup_log(&fixture.opener)?;
    anyhow::ensure!(
        log.len() == 1,
        "exactly one reactive top-up per pull, then give up — an upstream whose rate \
         outruns the working deposit is a pricing problem, and each extra attempt is a \
         transaction a waiting client pays for. Got {log:?}"
    );
    assert_counter(&fixture.metrics, "node_pull_reactive_topup_total", 1)?;

    fixture.shutdown().await?;
    Ok(())
}

// ===========================================================================
// ADR 041 — buy-side serve-economics gate + warming allowance.
// ===========================================================================

/// Provision B's `NodeOrigin` exactly like [`build_origin_with_probe_caches`] but
/// with a caller-chosen ADR 041 serve-economics policy, operator fee-share, base
/// sell rate, and a SHARED [`decdn_node::warming_allowance::WarmingAllowance`] the
/// test seeds/spends before the miss. Live probe + fresh caches, single provider.
/// `frequency_estimator` lets a test pin the `margin` policy's heat input
/// directly (standing in for the real observe-on-serve heat signal) instead of
/// driving real traffic to raise it; `None` reads cold (heat 0) like production
/// with no admission/eviction policy consuming an estimator.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
async fn build_origin_economics(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    buyer: Arc<dyn PoolOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    serve_economics: Arc<dyn decdn_node::serve_economics::ServeEconomicsPolicy>,
    operator_bps: u16,
    sell_rate_base: u64,
    warming: Arc<decdn_node::warming_allowance::WarmingAllowance>,
    frequency_estimator: Option<Arc<dyn decdn_cache::FrequencyEstimator>>,
) -> (NodeOrigin, CacheEngine, tempfile::TempDir) {
    let stakers = ConfigStakerSet::new(providers.iter().copied().collect());
    let mut dir = HashMap::new();
    dir.insert(U256::ZERO, providers);
    let (engine, engine_tmp) = throwaway_engine()
        .await
        .expect("throwaway engine for the economics fixture");
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(b_dht))),
        staker_set: Arc::new(stakers) as Arc<dyn StakerSet>,
        origin_directory: Arc::new(StaticOriginDirectory::new(dir)) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: b_dht,
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(local_rep),
        negative_cache: NegativeProbeCache::new(),
        probe_cache: PositiveProbeCache::new(),
        metrics: Arc::clone(metrics),
        registry_regions: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        config: NodeOriginConfig {
            probe_fanout: 5,
            // Generous by intent, like [`DEFAULT_TEST_PULL_DEADLINES`]: a loaded
            // runner must not end a pull this fixture is not measuring.
            pull_timeout: DEFAULT_TEST_PULL_DEADLINES.0,
            stall_window: DEFAULT_TEST_PULL_DEADLINES.1,
            min_throughput_bps: 0,
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            working_deposit: U256::ZERO,
            seller_reserve: U256::ZERO,
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
            serve_economics,
            operator_shares: decdn_node::fee_shares::OperatorShares::new(operator_bps),
            frequency_estimator,
            sell_rate_base,
            warming,
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        engine: engine.clone(),
    });
    (origin, engine, engine_tmp)
}

// ===========================================================================
// ADR 041 — integration + adversarial coverage.
// ===========================================================================

/// A frequency estimator pinned to one fixed value, so a test can drive the
/// `margin` policy's heat input directly (`N̂ = clamp(round(discount·heat), 1,
/// n_max)`) rather than having to generate enough real observe-on-serve traffic
/// to earn it. Standing in for "heat is already this high", not a production
/// shortcut — the production rule (heat rises only from real serves) is
/// untouched by this fixture.
#[derive(Debug)]
struct FixedHeat(u32);

impl decdn_cache::FrequencyEstimator for FixedHeat {
    fn observe(&self, _hash: Hash) {}

    fn estimate(&self, _hash: Hash) -> u32 {
        self.0
    }
}

/// Attack A (over-market loss bound): a malicious upstream quotes AT the
/// `margin` policy's amortized ceiling itself, with `heat` pinned (via
/// [`FixedHeat`], standing in for the real observe-on-serve heat signal) so `N̂ =
/// n_max`. At exactly that price the buy is NOT flagged speculative — the quote
/// sits AT the amortized floor, not above it — so the warming allowance is
/// never touched by this buy at all. That untracked zone is exactly what this
/// test bounds directly against the real ledger, rather than against
/// `WarmingAllowance` state that the attack never reaches.
///
/// Only ONE resale is realized (one downstream client fetch), so the buyer's
/// measured loss — what B paid A minus the operator margin B recovered on that
/// one resale — must not exceed `(operator_bps / 10_000) · P_sell · (n_max − 1)`
/// per whole MB of wire delivered: the gap between paying for `n_max` expected
/// resales (the heat-implied justification for the price) and recovering the
/// margin of just one.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn attack_a_over_market_loss_is_bounded() -> Result<()> {
    const N_MAX: u32 = 64;
    const DISCOUNT_BPS: u32 = 5000;
    // round(0.5 * 128) == 64 == N_MAX: pins N_hat exactly at the clamp ceiling.
    const HEAT: u32 = 128;
    const OP_BPS: u16 = 6000;
    // P_sell per MB. Kept well under `MAX_RATE_PER_MB` (1000) so the amortized
    // ceiling below stays a quotable wire rate: the attack quotes AT that
    // ceiling, so it must be <= the wire cap or the quote could never be signed.
    const SELL: u64 = 25;
    // amortized = op_bps * n_max * sell / 10_000 — the ceiling a malicious
    // upstream can quote AT without ever tripping the speculative flag.
    // = 6000 * 64 * 25 / 10_000 = 960, under the 1000 wire cap.
    const QUOTE_A: u64 = (OP_BPS as u64) * (N_MAX as u64) * SELL / 10_000;
    const LEDGER_DEPOSIT: u64 = 1_000_000_000;

    let payload = vec![0xADu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: the malicious upstream, quoting the ceiling exactly. -------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let ab_pool_id = B256::repeat_byte(0xA9);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        ab_pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(LEDGER_DEPOSIT),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let a_metrics = Arc::new(Metrics::new());
    let a_limiter = permissive_limiter(&a_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &a_metrics,
        a_limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        QUOTE_A,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        QUOTE_A,
    );

    // --- Node B: buys via NodeOrigin with heat pinned to N_MAX. -------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let warming = Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
        LEDGER_DEPOSIT,
        0,
    ));
    let serve_economics = Arc::new(decdn_node::serve_economics::MarginPolicy::new(
        DISCOUNT_BPS,
        N_MAX,
    )) as Arc<dyn decdn_node::serve_economics::ServeEconomicsPolicy>;
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        pool_id: ab_pool_id,
        deposit: U256::from(LEDGER_DEPOSIT),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_economics(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        serve_economics,
        OP_BPS,
        SELL,
        Arc::clone(&warming),
        Some(Arc::new(FixedHeat(HEAT)) as Arc<dyn decdn_cache::FrequencyEstimator>),
    )
    .await;

    // `Origin::fetch` runs the FULL buffered buy and admits the bytes into
    // B's cache — needed here so the downstream resale below actually has
    // something to serve.
    let bought = tokio::time::timeout(
        Duration::from_secs(20),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("origin fetch never returned"))?
    .map_err(|e| anyhow::anyhow!("origin fetch: {e}"))?;
    anyhow::ensure!(
        matches!(bought, OriginFetch::AlreadyAdmitted),
        "the attack quotes AT the ceiling; the gate must admit it, got {bought:?}"
    );
    anyhow::ensure!(
        engine.get(hash).await?.as_ref() == payload.as_slice(),
        "B's cache must hold the bought bytes"
    );

    // --- Downstream: ONE real client resells the blob at market. ------------
    let b_eth = Arc::new(PrivateKeySigner::random());
    let client_signer = Arc::new(PrivateKeySigner::random());
    let b_pool_id = B256::repeat_byte(0xB9);
    let store_b = Arc::new(MemoryPoolStateStore::new());
    let b_lane = decdn_incentive::LaneKey {
        pool_id: b_pool_id,
        signer: client_signer.address(),
        provider: b_eth.address(),
    };
    store_b.record(&LaneState::hydrate(
        b_pool_id,
        client_signer.address(),
        b_eth.address(),
        U256::from(LEDGER_DEPOSIT),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let handler_b = build_handler_full_configured(
        b_id,
        &b_eth,
        &b_metrics,
        permissive_limiter(&b_metrics),
        engine,
        store_b.clone() as Arc<dyn PoolStateStore>,
        SELL,
        &domains,
        16,
        |deps| {
            deps.warming_credit = Arc::new(
                decdn_node::warming_allowance::DirectWarmingCreditSink::new(Arc::clone(&warming)),
            );
            deps.operator_shares = decdn_node::fee_shares::OperatorShares::new(OP_BPS);
        },
    )?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let client_sk = fresh_key();
    let client_id = client_sk.public();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    let binding = decdn_client::sign_client_binding(
        &client_signer,
        B256::from(*client_id.as_bytes()),
        &binding_dom(),
    )?;
    let ctx_client_to_b = PoolContext {
        pool_id: b_pool_id,
        provider: b_eth.address(),
        deposit: U256::from(LEDGER_DEPOSIT),
        client_signer: Arc::clone(&client_signer),
        voucher_domain: voucher_dom(),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: Some(binding),
        capability: None,
    };
    let got = decdn_client::stream_fetch(
        &client_ep,
        target_b,
        &ctx_client_to_b,
        &slash_domain(),
        b_eth.address(),
        *hash.as_bytes(),
        0,
        0x00a1_0001,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(got.as_ref() == payload.as_slice(), "resale bytes mismatch");

    // --- Measure: what B paid A, vs. the margin it recovered on the resale. --
    let paid_out = progress_log(&recorded)?
        .last()
        .copied()
        .map(|(_, _, amount)| amount)
        .ok_or_else(|| anyhow::anyhow!("B never recorded a payment to A"))?;
    let paid_in = store_b
        .get(b_lane)?
        .ok_or_else(|| anyhow::anyhow!("B<->client lane vanished"))?
        .last_amount();
    let margin_recovered = paid_in.saturating_mul(U256::from(OP_BPS)) / U256::from(10_000u64);
    let loss = paid_out.saturating_sub(margin_recovered);

    let wire = support::bao_wire_len_whole(total_bytes);
    let mb_wire = wire.div_ceil(MB_BYTES);
    let bound = U256::from(OP_BPS) * U256::from(SELL) * U256::from(u64::from(N_MAX) - 1)
        / U256::from(10_000u64)
        * U256::from(mb_wire);
    anyhow::ensure!(
        loss <= bound,
        "Attack A loss bound violated: paid_out {paid_out}, paid_in {paid_in}, margin \
         recovered {margin_recovered}, measured loss {loss} > bound {bound} \
         (op_bps/10_000 * sell * (n_max-1) * {mb_wire} MB)"
    );
    // Sanity: the attack IS lossy — else the bound above is vacuous.
    anyhow::ensure!(
        loss > U256::ZERO,
        "the attack must be genuinely lossy for the bound above to mean anything, \
         got paid_out {paid_out}, margin_recovered {margin_recovered}"
    );

    shutdown([task_a, task_b], [&client_ep, &ep_b, &ep_a]).await?;
    Ok(())
}

/// One ADR 041 Attack-B cycle: source `a_sk` (its identity persists across
/// calls that clone the same key in, so its warming bucket persists too) holds
/// a blob distinguished by `salt` (distinct content ⇒ distinct hash and pool
/// ids), quoted at `quote`. B buys it through `NodeOrigin` sharing `warming`
/// with the caller, then — only if the buy was admitted — serves it downstream
/// to `serves` distinct one-shot clients from the SAME cache (no re-buy: the
/// blob is already admitted).
///
/// Returns `(admitted, metrics)`: `admitted` is whether the buy was let
/// through (`Origin::fetch`'s `OriginFetch::AlreadyAdmitted`); a refused buy
/// skips serving (there is nothing bought to serve). `Origin::fetch` collapses
/// `PullMiss::BelowMargin` and a clean miss to the same wire-identical
/// `NotFound`, so a caller that needs to confirm a refusal was specifically the
/// ADR 041 gate (not e.g. an unrelated transport fault) reads `metrics`'
/// `serve_economics_refused_total` back.
#[allow(
    clippy::too_many_arguments,
    clippy::expect_used,
    clippy::too_many_lines
)]
async fn attack_b_attempt(
    a_sk: iroh::SecretKey,
    salt: u8,
    quote: u64,
    sell: u64,
    op_bps: u16,
    discount_bps: u32,
    n_max: u32,
    warming: &Arc<decdn_node::warming_allowance::WarmingAllowance>,
    serves: u32,
) -> Result<(bool, Arc<Metrics>)> {
    const LEDGER_DEPOSIT: u64 = 1_000_000_000;
    let payload = vec![salt; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let ab_pool_id = B256::repeat_byte(salt);
    let store_a = Arc::new(MemoryPoolStateStore::new());
    store_a.record(&LaneState::hydrate(
        ab_pool_id,
        b_buyer.address(),
        a_eth.address(),
        U256::from(LEDGER_DEPOSIT),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let a_metrics = Arc::new(Metrics::new());
    let a_limiter = permissive_limiter(&a_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &a_metrics,
        a_limiter,
        cache_a,
        store_a as Arc<dyn PoolStateStore>,
        quote,
        &domains,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        total_bytes,
        quote,
    );

    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let serve_economics = Arc::new(decdn_node::serve_economics::MarginPolicy::new(
        discount_bps,
        n_max,
    )) as Arc<dyn decdn_node::serve_economics::ServeEconomicsPolicy>;
    let buyer = Arc::new(StubOpener {
        pool_id: ab_pool_id,
        deposit: U256::from(LEDGER_DEPOSIT),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn PoolOpener>;
    let (origin, engine, _engine_tmp) = build_origin_economics(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        serve_economics,
        op_bps,
        sell,
        Arc::clone(warming),
        None,
    )
    .await;

    // `Origin::fetch` runs the full buffered buy and admits the bytes into
    // B's cache, needed so a refused-vs-admitted
    // flood attempt can go on to actually serve downstream when admitted.
    // Whether a refusal was specifically the ADR 041 gate (`BelowMargin`, wire-
    // identical to a clean miss) is read back from `serve_economics_refused`,
    // since `Origin::fetch`'s `OriginFetch` collapses both to `NotFound`.
    let bought = tokio::time::timeout(
        Duration::from_secs(20),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("origin fetch never returned"))?
    .map_err(|e| anyhow::anyhow!("origin fetch: {e}"))?;
    let admitted = matches!(bought, OriginFetch::AlreadyAdmitted);

    if admitted && serves > 0 {
        let b_eth = Arc::new(PrivateKeySigner::random());
        let store_b = Arc::new(MemoryPoolStateStore::new());
        let mut lanes = Vec::new();
        for i in 0..serves {
            let client_signer = Arc::new(PrivateKeySigner::random());
            let mut pool_id_bytes = [0u8; 32];
            pool_id_bytes[0] = salt;
            pool_id_bytes[1] = u8::try_from(i).unwrap_or(0xFF);
            let pool_id = B256::from(pool_id_bytes);
            store_b.record(&LaneState::hydrate(
                pool_id,
                client_signer.address(),
                b_eth.address(),
                U256::from(LEDGER_DEPOSIT),
                0,
                U256::ZERO,
                U256::ZERO,
                None,
                decdn_incentive::LaneChain::NONE,
            ))?;
            lanes.push((pool_id, client_signer));
        }
        let handler_b = build_handler_full_configured(
            b_id,
            &b_eth,
            &b_metrics,
            permissive_limiter(&b_metrics),
            engine,
            store_b as Arc<dyn PoolStateStore>,
            sell,
            &domains,
            16,
            |deps| {
                deps.warming_credit =
                    Arc::new(decdn_node::warming_allowance::DirectWarmingCreditSink::new(
                        Arc::clone(warming),
                    ));
                deps.operator_shares = decdn_node::fee_shares::OperatorShares::new(op_bps);
            },
        )?;
        let task_b = spawn_server(ep_b.clone(), handler_b);
        for (i, (pool_id, client_signer)) in lanes.into_iter().enumerate() {
            let client_sk = fresh_key();
            let client_id = client_sk.public();
            let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
            let target_b = EndpointAddr::new(b_id).with_ip_addr(addr_b);
            let binding = decdn_client::sign_client_binding(
                &client_signer,
                B256::from(*client_id.as_bytes()),
                &binding_dom(),
            )?;
            let ctx = PoolContext {
                pool_id,
                provider: b_eth.address(),
                deposit: U256::from(LEDGER_DEPOSIT),
                client_signer: Arc::clone(&client_signer),
                voucher_domain: voucher_dom(),
                prior_bytes_delivered: U256::ZERO,
                prior_amount: U256::ZERO,
                client_binding: Some(binding),
                capability: None,
            };
            let got = decdn_client::stream_fetch(
                &client_ep,
                target_b,
                &ctx,
                &slash_domain(),
                b_eth.address(),
                *hash.as_bytes(),
                0,
                0x00b0_0000u64
                    .saturating_add(u64::from(salt) * 0x100)
                    .saturating_add(i as u64),
                Duration::from_secs(20),
            )
            .await?;
            anyhow::ensure!(
                got.as_ref() == payload.as_slice(),
                "resale #{i} bytes mismatch"
            );
            shutdown([], [&client_ep]).await?;
        }
        shutdown([task_a, task_b], [&ep_b, &ep_a]).await?;
        return Ok((admitted, b_metrics));
    }

    shutdown([task_a], [&ep_b, &ep_a]).await?;
    Ok((admitted, b_metrics))
}

/// Attack B (per-source dud-flood bound + vindication): (a) flooding distinct
/// at-market one-hit blobs from ONE source spends its warming allowance `B`
/// down by each dud's loss. A single dud does not cut the source off — the
/// budget covers it — but once the duds exhaust `B`, every FURTHER at-market
/// cold buy from that same source refuses `BelowMargin` rather than buying. The
/// buys run one at a time here, so exactly one buy overshoots zero; concurrent
/// pulls from one source can each overshoot (see
/// `WarmingAllowance::debit_speculative`). A second, independent source is
/// completely untouched by the first source's flood. (b) A blob re-served twice
/// from a source refunds its buy (serve-vindicated), so an honest, popular
/// source keeps warming even from a mostly-spent allowance.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn attack_b_dud_flood_is_bounded_and_vindication_works() -> Result<()> {
    const N_MAX: u32 = 64;
    const DISCOUNT_BPS: u32 = 5000;
    const OP_BPS: u16 = 6000;
    const SELL: u64 = 1000;
    // heat 0 -> n_hat 1 -> amortized 600; a market quote of 1000 sits ABOVE the
    // amortized floor, so a fresh (warm) source's buy is flagged speculative and
    // debited the full buy cost — the regime this attack lives in.
    const QUOTE_MARKET: u64 = 1000;
    // `B` is exactly ONE market buy of the 1.5 MiB `PAYLOAD_LEN` blob, billed as
    // 2 whole MB: a buy debits 1000·2 = 2000, and each downstream serve credits
    // 600·2 = 1200.
    // One dud served once leaves 1200 (still warm); a second, unserved dud
    // takes the source to -800 (spent).
    const BUDGET: u64 = 2000;

    let warming = Arc::new(decdn_node::warming_allowance::WarmingAllowance::new(
        BUDGET, 0,
    ));

    // Source A: the first buy is a dud — bought at market, served exactly once.
    let src1_sk = fresh_key();
    let src1_id = decdn_node::warming_allowance::SourceId::from_bytes(*src1_sk.public().as_bytes());
    let (first_admitted, _) = attack_b_attempt(
        src1_sk.clone(),
        0xB1,
        QUOTE_MARKET,
        SELL,
        OP_BPS,
        DISCOUNT_BPS,
        N_MAX,
        &warming,
        1,
    )
    .await?;
    anyhow::ensure!(
        first_admitted,
        "the first (dud) at-market buy from a fresh source must be admitted"
    );
    anyhow::ensure!(
        warming.available(src1_id),
        "one dud (bought at market, served once) costs only its fee skim; the budget \
         covers it and the source keeps warming"
    );

    // A second dud from the same source is still admitted, and it spends the
    // budget: the source now reads as spent.
    let (second_dud_admitted, _) = attack_b_attempt(
        src1_sk.clone(),
        0xB2,
        QUOTE_MARKET,
        SELL,
        OP_BPS,
        DISCOUNT_BPS,
        N_MAX,
        &warming,
        0,
    )
    .await?;
    anyhow::ensure!(
        second_dud_admitted,
        "a dud while the source's allowance is still positive must be admitted"
    );
    anyhow::ensure!(
        !warming.available(src1_id),
        "the duds must exhaust the budget and cut the source off"
    );

    // Flood: further distinct cold blobs from the SAME, spent source refuse
    // BelowMargin at the amortized floor rather than buying — the loss stops.
    for salt in [0xB3u8, 0xB4u8] {
        let (flood_admitted, flood_metrics) = attack_b_attempt(
            src1_sk.clone(),
            salt,
            QUOTE_MARKET,
            SELL,
            OP_BPS,
            DISCOUNT_BPS,
            N_MAX,
            &warming,
            0,
        )
        .await?;
        anyhow::ensure!(
            !flood_admitted,
            "a further at-market cold buy from a spent source must refuse (salt {salt:#x})"
        );
        assert_counter(&flood_metrics, "serve_economics_refused_total", 1)?;
    }

    // An independent second source: untouched by A's flood.
    let src2_sk = fresh_key();
    let src2_id = decdn_node::warming_allowance::SourceId::from_bytes(*src2_sk.public().as_bytes());
    anyhow::ensure!(
        warming.available(src2_id),
        "an independent source must read available before it is ever touched"
    );
    let (second_admitted, _) = attack_b_attempt(
        src2_sk,
        0xC1,
        QUOTE_MARKET,
        SELL,
        OP_BPS,
        DISCOUNT_BPS,
        N_MAX,
        &warming,
        0,
    )
    .await?;
    anyhow::ensure!(
        second_admitted,
        "a second source's allowance must be untouched by another source's flood"
    );

    // (b) Vindication: a THIRD source that earlier duds already spent down to
    // 500 buys once (-1500) and is served TWICE. One serve alone leaves it at
    // -300 (spent), so only the second serve's credit (+900) keeps it warming.
    let src3_sk = fresh_key();
    let src3_id = decdn_node::warming_allowance::SourceId::from_bytes(*src3_sk.public().as_bytes());
    warming.debit_speculative(
        src3_id,
        decdn_cache::Hash::from_bytes([0xD0u8; 32]),
        BUDGET - 500,
    );
    anyhow::ensure!(
        warming.available(src3_id),
        "precondition: the earlier duds leave the source warm"
    );
    let (vindicated_admitted, _) = attack_b_attempt(
        src3_sk,
        0xD1,
        QUOTE_MARKET,
        SELL,
        OP_BPS,
        DISCOUNT_BPS,
        N_MAX,
        &warming,
        2,
    )
    .await?;
    anyhow::ensure!(vindicated_admitted, "the vindication buy must be admitted");
    anyhow::ensure!(
        warming.available(src3_id),
        "a blob re-served twice must refund its buy and keep the source warming"
    );

    Ok(())
}

// ===========================================================================
// #1506 — two-holder ranged assembly over the REAL paid path (`PeerRunSink`).
//
// Every other multi-holder test in the tree drives `node_origin::ranged_pull`'s
// pure loop through the `FakeSink` (which scripts run outcomes and never opens a
// lane). This one drives the PAID half: node S serve-misses a blob held only as
// A:{block 0} + B:{block 1}, so `run_pull_leg`'s `PeerRunSink` opens a real
// buyer lane to EACH holder in turn — voucher payment, settle/watermark
// hand-off, and cross-run shared-pool solvency — the surface where shared-pool
// accounting, per-run range clamping, and run cancel can go wrong (#1506). The blob is
// two DISCOVERY-block-sized slices; the block size is overridden to 16 KiB
// (`override_discovery_block_bytes_for_test`) so "two blocks" is a 32 KiB blob,
// not the 128 MiB a real two-block blob would need — the whole point of the
// test-support seam. Both holders hold the whole (tiny) blob but ADVERTISE
// partial coverage, so the planner concentrates each block onto its sole coverer
// and the two lanes run sequentially on one shared pool.
// ===========================================================================

/// Two providers for `NodeOrigin`'s discovery: `(dht, operator)` for each of A
/// and B. The ranged-assembly gather walks both because their coverage union is
/// what spans the blob.
fn two_providers(
    a_dht: DhtNodeId,
    a_eth: Address,
    b_dht: DhtNodeId,
    b_eth: Address,
) -> (Vec<DhtNodeId>, HashMap<DhtNodeId, Address>) {
    let mut addr_map = HashMap::new();
    addr_map.insert(a_dht, a_eth);
    addr_map.insert(b_dht, b_eth);
    (vec![a_dht, b_dht], addr_map)
}

/// Spin up one PARTIAL holder: it holds the whole (tiny) blob in a real cache and
/// serves any range over the real [`ClientHandler`], but its probe advertises
/// only `coverage` — the discovery blocks the planner may route to it. Seeds its
/// seller lane on the SHARED `ab_pool_id` keyed by the buyer's signer and this
/// holder's own operator address, so a test can read back exactly what this
/// holder was paid.
async fn spawn_partial_holder(
    payload: &[u8],
    ab_pool_id: B256,
    s_buyer_addr: Address,
    coverage: Coverage,
) -> Result<(
    iroh::PublicKey,
    std::net::SocketAddr,
    Address,
    iroh::Endpoint,
    tokio::task::JoinHandle<()>,
    Arc<MemoryPoolStateStore>,
)> {
    let hash = Hash::new(payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let (cache, hash_h, tmp) = cache_with_blob(payload).await?;
    anyhow::ensure!(hash_h == hash, "holder fixture hash mismatch");
    std::mem::forget(tmp);

    let sk = fresh_key();
    let id = sk.public();
    let eth = Arc::new(PrivateKeySigner::random());
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        ab_pool_id,
        s_buyer_addr,
        eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler = build_handler_full(
        id,
        &eth,
        &metrics,
        limiter,
        cache,
        store.clone() as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
    )?;
    let (ep, addr) = local_endpoint(sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    // Accept loop: real `ClientHandler` for the paid pull, a coverage-carrying
    // probe responder for `cdn/probe/v1`.
    let task = {
        use iroh::protocol::ProtocolHandler;
        let ep = ep.clone();
        let eth = Arc::clone(&eth);
        let slash = slash_domain();
        tokio::spawn(async move {
            while let Some(incoming) = ep.accept().await {
                let Ok(connecting) = incoming.accept() else {
                    continue;
                };
                let Ok(conn) = connecting.await else { continue };
                if conn.alpn() == ALPN_PROBE {
                    let eth = Arc::clone(&eth);
                    let dom = slash.clone();
                    let coverage = coverage.clone();
                    tokio::spawn(async move {
                        let _ = answer_probe_with_coverage(
                            conn,
                            &eth,
                            &dom,
                            RATE,
                            total_bytes,
                            coverage,
                        )
                        .await;
                    });
                } else {
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        let _ = decdn_node::handlers::client::ClientProtocol::new(handler)
                            .accept(conn)
                            .await;
                    });
                }
            }
        })
    };
    Ok((id, addr, eth.address(), ep, task, store))
}

/// Build serving node S: an empty-cache window-paced `ClientHandler` whose
/// `NodeOrigin` discovers the two partial holders, plus the leaf's own lane in
/// S's seller store. A focused twin of [`build_node_b_with_leaves`] for the
/// two-holder ranged pull (that helper hardwires a single provider).
#[allow(clippy::too_many_arguments)]
async fn build_serving_node(
    hash: Hash,
    ab_pool_id: B256,
    s_buyer: &Arc<PrivateKeySigner>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    holder_dials: &[(iroh::PublicKey, std::net::SocketAddr)],
    leaf_channel_id: B256,
    leaf_eth_addr: Address,
    leaf_deposit: U256,
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Address,
)> {
    let s_sk = fresh_key();
    let s_id = s_sk.public();
    let s_eth = Arc::new(PrivateKeySigner::random());
    let (ep_s, addr_s) = local_endpoint(s_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    // Prime S's iroh address cache for every holder so NodeId-only dialing in the
    // pull resolves each one.
    for (holder_id, holder_addr) in holder_dials {
        let _ = probe_once(
            &ep_s,
            EndpointAddr::new(*holder_id).with_ip_addr(*holder_addr),
            *hash.as_bytes(),
            1,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let s_metrics = Arc::new(Metrics::new());
    let (origin, _engine, recorded, _engine_tmp) = provisioned_origin_with_deadlines(
        &ep_s,
        DhtNodeId::from_bytes(*s_id.as_bytes()),
        hash,
        ab_pool_id,
        s_buyer,
        &local_rep,
        &s_metrics,
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES,
        0, // max_blob_size_bytes: 0 = unlimited; this test does not exercise the size ceiling
    )
    .await;

    let cache_tmp = tempfile::tempdir()?;
    let cache_s = decdn_cache::CacheEngine::open(cache_tmp.path(), vec![], 64).await?;
    let cache_handle = cache_s.clone();
    std::mem::forget(cache_tmp);
    let store_s = Arc::new(MemoryPoolStateStore::new());
    store_s.record(&LaneState::hydrate(
        leaf_channel_id,
        leaf_eth_addr,
        s_eth.address(),
        leaf_deposit,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let mut pool_status_map: HashMap<B256, decdn_node::pool_view::PoolStatus> = HashMap::new();
    pool_status_map.insert(
        leaf_channel_id,
        decdn_node::pool_view::PoolStatus {
            owner: leaf_eth_addr,
            remaining: leaf_deposit,
            lifecycle: decdn_node::pool_view::Lifecycle::Open,
        },
    );
    let pool_view = Arc::new(StubPoolView {
        status: pool_status_map,
    }) as Arc<dyn decdn_node::pool_view::PoolView>;
    let limiter = permissive_limiter(&s_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_s = build_handler_full_configured(
        s_id,
        &s_eth,
        &s_metrics,
        limiter,
        cache_s,
        store_s as Arc<dyn PoolStateStore>,
        RATE,
        &domains,
        16,
        |deps| {
            deps.pull_through = Some(Duration::from_secs(20));
            deps.pull_through_origin = Some(Arc::new(origin));
            deps.pool_view = Some(pool_view);
        },
    )?;
    let target = EndpointAddr::new(s_id).with_ip_addr(addr_s);
    Ok((
        handler_s,
        target,
        ep_s,
        recorded,
        cache_handle,
        s_eth.address(),
    ))
}

/// #1506: node S assembles a blob held only as A:{block 0} + B:{block 1} across
/// two sequential paid lanes on one shared pool, and each holder is paid only for
/// ITS block.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn two_partial_holders_assemble_over_the_real_paid_path() -> Result<()> {
    // Tiny discovery blocks so "two blocks" is a 32 KiB blob, not 128 MiB. The
    // guard reverts the override when the test ends; `cargo nextest` runs each
    // test in its own process, so no sibling test sees the override.
    let block: u64 = 16 * 1024;
    let _block_guard = decdn_protocol::override_discovery_block_bytes_for_test(block);
    anyhow::ensure!(
        decdn_protocol::discovery_block_bytes() == block,
        "override did not take"
    );

    // A two-block, position-varying blob (a permuted chunk would still equal a
    // uniform fill, so vary by index).
    let payload_len = usize::try_from(2 * block).unwrap_or(usize::MAX);
    let payload: Vec<u8> = (0..payload_len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let hash = Hash::new(&payload);
    let total_bytes = 2 * block;
    anyhow::ensure!(
        decdn_protocol::num_blocks(total_bytes) == 2,
        "the blob must span exactly two discovery blocks under the override"
    );

    let ab_pool_id = B256::repeat_byte(0xA1);
    let s_buyer = Arc::new(PrivateKeySigner::random());

    // Holder A covers ONLY block 0; holder B ONLY block 1. Both hold the whole
    // blob and can serve any range — only their ADVERTISED coverage is partial.
    let (a_id, a_addr, a_eth, ep_a, task_a, store_a) = spawn_partial_holder(
        &payload,
        ab_pool_id,
        s_buyer.address(),
        Coverage::from_block_indices(2, [0].into_iter()),
    )
    .await?;
    let (b_id, b_addr, b_eth, ep_b, task_b, store_b) = spawn_partial_holder(
        &payload,
        ab_pool_id,
        s_buyer.address(),
        Coverage::from_block_indices(2, [1].into_iter()),
    )
    .await?;

    let (providers, addr_map) = two_providers(
        DhtNodeId::from_bytes(*a_id.as_bytes()),
        a_eth,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        b_eth,
    );

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x1F);
    let (handler_s, s_target, ep_s, recorded, cache_s, s_operator) = build_serving_node(
        hash,
        ab_pool_id,
        &s_buyer,
        providers,
        addr_map,
        &[(a_id, a_addr), (b_id, b_addr)],
        leaf_channel_id,
        leaf_eth.address(),
        U256::from(DEPOSIT_MICRO_USDC),
    )
    .await?;
    let task_s = spawn_server(ep_s.clone(), handler_s);

    // The leaf pulls the whole blob from S; S serve-misses and assembles it from
    // both holders.
    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let outcome = leaf_paced_pull(
        &leaf_ep,
        s_target,
        leaf_node_id,
        &leaf_eth,
        s_operator,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await?;

    anyhow::ensure!(outcome.completed, "leaf delivery did not complete");
    anyhow::ensure!(outcome.hash_ok, "leaf received bytes failed the hash check");
    anyhow::ensure!(
        outcome.received == total_bytes,
        "leaf received {} of {total_bytes} bytes",
        outcome.received
    );
    // S cached the assembled, verified blob — it is now a holder for future pulls.
    anyhow::ensure!(
        cache_s.has(hash).await?,
        "S must promote the assembled blob on a complete delivery"
    );

    // The headline: BOTH holders' seller lanes advanced, so the assembly opened a
    // real paid lane to each — not one holder serving everything.
    let lane_a = LaneKey {
        pool_id: ab_pool_id,
        signer: s_buyer.address(),
        provider: a_eth,
    };
    let lane_b = LaneKey {
        pool_id: ab_pool_id,
        signer: s_buyer.address(),
        provider: b_eth,
    };
    let a_delivered = store_a
        .get(lane_a)?
        .ok_or_else(|| anyhow::anyhow!("holder A lane vanished"))?
        .last_bytes_delivered();
    let b_delivered = store_b
        .get(lane_b)?
        .ok_or_else(|| anyhow::anyhow!("holder B lane vanished"))?
        .last_bytes_delivered();

    // Each holder is paid ONLY for its own block's wire: A the [0, block) range,
    // B the [block, total) range — never the whole blob (#1506). Bao meters
    // WIRE (content + interleaved proof), so compare against each range's own
    // verified-stream encoding.
    let a_wire =
        u64::try_from(honest_bao_wire_range(&payload, 0, block)?.len()).unwrap_or(u64::MAX);
    let b_wire =
        u64::try_from(honest_bao_wire_range(&payload, block, 0)?.len()).unwrap_or(u64::MAX);
    let whole_wire =
        u64::try_from(honest_bao_wire_range(&payload, 0, 0)?.len()).unwrap_or(u64::MAX);
    anyhow::ensure!(
        a_delivered == U256::from(a_wire),
        "holder A must be paid for exactly block 0's wire ({a_wire}), got {a_delivered}"
    );
    anyhow::ensure!(
        b_delivered == U256::from(b_wire),
        "holder B must be paid for exactly block 1's wire ({b_wire}), got {b_delivered}"
    );
    anyhow::ensure!(
        a_delivered < U256::from(whole_wire) && b_delivered < U256::from(whole_wire),
        "neither holder may be paid for the whole blob: A={a_delivered}, B={b_delivered}, \
         whole={whole_wire}"
    );

    // S's buyer side persisted a watermark for BOTH providers on the one shared
    // pool (#852, #1506): two distinct providers, each recorded.
    let paid_providers: std::collections::HashSet<Address> = progress_log(&recorded)?
        .into_iter()
        .map(|(p, ..)| p)
        .collect();
    anyhow::ensure!(
        paid_providers == std::collections::HashSet::from([a_eth, b_eth]),
        "both holders' lanes must have persisted a watermark, got {paid_providers:?}"
    );

    shutdown([task_s, task_a, task_b], [&leaf_ep, &ep_s, &ep_a, &ep_b]).await?;
    Ok(())
}
