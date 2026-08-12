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
    Bytes, CacheEngine, CacheMetrics, CircuitBreakerPolicy, Hash, Percent, PinnedHashes,
    RetryPolicy,
};
use decdn_common::admin::RegionBytes;
use decdn_incentive::{
    ChannelOpenFailureReason, ChannelState, ChannelStateStore, EPHEMERAL_BINDING_NONCE,
    MemoryChannelStateStore, ProbeSlashData, StreamSlashData, Voucher, bind_node_id_domain,
    binding_signing_hash, signed_to_wire_voucher, slash_judge_domain, voucher_domain,
};
use decdn_node::buyer_channel::{
    ChannelOpenPending, ChannelOpener, OpenReported, OpenSlotReserved,
};
use decdn_node::client_requester::probe::probe_once;
use decdn_node::client_requester::{
    ChannelContext, ChannelLedger, Cumulative, LocalPullFault, PullDeadlines, stream_fetch_shared,
};
use decdn_node::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use decdn_node::dht::{
    ConfigStakerSet, NegativeProbeCache, NodeAddressResolver, OriginDirectory, PositiveProbeCache,
    ProbedProvider, StakerSet, StaticNodeAddressDirectory, StaticOriginDirectory,
};
use decdn_node::leech_governor::{LeechCaps, LeechCapsConfig, LeechGovernor};
use decdn_node::metrics::Metrics;
use decdn_node::node_origin::{NodeOrigin, NodeOriginConfig, NodeOriginDeps, TeeVerdict};
use decdn_node::region_accounting::{RegionAccountant, RegionResolver};
use decdn_node::selection::{MAX_PROVIDER_ATTEMPTS, outer_pull_deadline};
use decdn_protocol::client::{
    ChunkData, ClientBinding, ClientMessage, StreamError, StreamRequest, StreamRequestExt,
    StreamResponse, StreamResponseBody, VoucherRejectReason,
};
use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};
use decdn_protocol::{
    ALPN_CLIENT, ALPN_PROBE, CHUNK_SIZE, ContentHash, DEFAULT_VOUCHER_INTERVAL_MB, MB_BYTES,
    ProbeMessage, decode_message, encode_message, encode_stream_request, read_frame, write_frame,
};
use decdn_reputation::{LocalReputation, LocalReputationConfig};
use iroh::EndpointAddr;
use iroh::endpoint::Connection;

mod support;
use support::{
    HandlerDomains, build_handler_full, build_handler_full_configured, cache_with_blob,
    empty_cache, fresh_key, local_endpoint, permissive_limiter, spawn_server,
};

const CHAIN_ID: u64 = 421_614;
const TOKEN: Address = Address::repeat_byte(0x22);
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// 1.5 MiB → crosses one 1-MiB voucher interval plus a closing voucher.
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

/// A recorded `record_progress` call: `(provider, nonce, bytes_delivered, amount)`.
type ProgressEntry = (Address, U256, U256, U256);

/// A buyer-channel opener that stands in for the chain-backed
/// `BuyerChannelService`, so the test exercises the pull without a chain.
///
/// It also models the #852 persistence loop: [`ChannelOpener::record_progress`]
/// appends to `recorded`, and `open_or_reuse_channel` seeds the returned
/// context's `prior_*` from the latest recorded entry for that provider — exactly
/// what the real store-backed service does on reuse. A second pull therefore
/// resumes from the first pull's watermark instead of re-signing a stale voucher.
#[derive(Debug)]
struct StubOpener {
    channel_id: B256,
    token: Address,
    deposit: U256,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    /// `record_progress` calls in order — the test's view of what was persisted.
    recorded: Arc<Mutex<Vec<ProgressEntry>>>,
    /// `retire_channel` calls in order — the test's view of which channels were
    /// rotated out after an upstream said they could never pay again (#1145 review).
    ///
    /// Retirement is modelled, not just logged: once a provider's channel is retired,
    /// `open_or_reuse_channel` stops resuming from its persisted watermark and hands
    /// back a fresh (zeroed) context, which is what the store-backed service does once
    /// the row is gone. A test can therefore tell a channel that was *recorded* as
    /// retired from one that actually stopped being reused.
    retired: Arc<Mutex<Vec<(Address, B256)>>>,
}

#[async_trait]
impl ChannelOpener for StubOpener {
    async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        _deposit_hint: U256,
        _budget: Duration,
    ) -> Result<ChannelContext> {
        // A retired channel is GONE: the store row was dropped, so there is nothing to
        // resume from and the next open starts clean. Modelling this is what lets a test
        // distinguish "we retired the channel" from "we retired it and then resumed the
        // dead watermark anyway", which would wedge the fresh channel exactly as the old
        // one was (#1145 review).
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
        // zeros if none) — the reuse path the #852 fix makes correct.
        let (prior_nonce, prior_bytes_delivered, prior_amount) = recorded
            .iter()
            .rev()
            .find(|(provider, ..)| *provider == provider_addr)
            .filter(|_| !was_retired)
            .map_or((U256::ZERO, U256::ZERO, U256::ZERO), |(_, n, b, a)| {
                (*n, *b, *a)
            });
        Ok(ChannelContext {
            channel_id: self.channel_id,
            token: self.token,
            deposit: self.deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_nonce,
            prior_bytes_delivered,
            prior_amount,
            client_binding: None,
        })
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        channel_id: B256,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
        // The orchestrator must persist progress against the channel it pulled
        // on — i.e. the id from the `ChannelContext` it just opened/reused.
        // Asserts the `ctx.channel_id` plumbing at the pull call site (#838).
        anyhow::ensure!(
            channel_id == self.channel_id,
            "record_progress channel_id {channel_id} != opened channel {}",
            self.channel_id
        );
        self.recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?
            .push((provider_addr, nonce, bytes_delivered, amount));
        Ok(())
    }

    fn retire_channel(&self, provider_addr: Address, channel_id: B256) -> Result<bool> {
        // Compare-and-delete, like the store: a channel id that is not the one we handed
        // out is a row some newer open already replaced, and retiring it is not ours to do.
        if channel_id != self.channel_id {
            return Ok(false);
        }
        self.retired
            .lock()
            .map_err(|_| anyhow::anyhow!("retired lock poisoned"))?
            .push((provider_addr, channel_id));
        Ok(true)
    }

    fn channel_expiry(&self, _provider_addr: Address) -> Option<u64> {
        // A far-future expiry (year ~2286) so a wedge records a LIVE provider-wide suppression
        // horizon (#1145 review) — otherwise `None` would leave only per-(peer, hash) cover and
        // the wedged-provider filter could never be exercised.
        Some(9_999_999_999)
    }
}

/// An opener that hands back a fresh-channel context but whose `record_progress`
/// always fails — models a store-write failure on the persist path so a test can
/// assert the pull still delivers the paid-for bytes (#852: a persist failure
/// must not fail the pull, only surface via the metric + warn).
#[derive(Debug)]
struct FailingRecordOpener {
    channel_id: B256,
    token: Address,
    deposit: U256,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
}

/// A [`ChannelOpener`] whose open for `wedged` never completes within the caller's
/// budget — the on-chain hazard #1143 exists for (an unresponsive RPC, a
/// `ChannelOpened` tx that never mines). Every other provider opens instantly.
///
/// It ASSUMES the budget contract rather than testing it: it sleeps for `budget`
/// and hands back the typed [`ChannelOpenPending`], which is what the real service
/// does — but because this body *re-implements* that behaviour, nothing here would
/// notice if `BuyerChannelService::open_or_reuse_channel` stopped doing it. Scope
/// this fixture to what it genuinely covers: `node_origin`'s candidate loop, i.e.
/// that a pending open is metered, scores no reputation, and falls through to the
/// next candidate. Returning the real sentinel (rather than a bare string) is what
/// makes it reach `record_channel_open_failure`'s pending arm and its counter
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
    channel_id: B256,
    token: Address,
    deposit: U256,
    signer: Arc<PrivateKeySigner>,
    voucher_domain: Eip712Domain,
    /// Providers whose open was attempted, in order — so a test can prove the loop
    /// actually reached the fallback rather than succeeding for some other reason.
    attempted: Arc<Mutex<Vec<Address>>>,
    /// Which typed sentinel a wedged provider raises. The two are handled by DIFFERENT
    /// arms of `record_channel_open_failure` and mean different things, so a fixture that
    /// could only produce one of them left the other arm unexercised (#1145 review).
    stall: OpenStall,
}

/// The two ways an open can fail to hand back a channel WITHOUT anything being wrong.
#[derive(Debug, Clone, Copy)]
enum OpenStall {
    /// The open outlived the caller's budget and continues in a detached task (#1143).
    Pending,
    /// A reconcile scan holds this provider's open slot while it re-hydrates the row, and
    /// tells us to retry. Self-clearing — and it happens at EVERY boot, which is what makes
    /// its arm load-bearing: counting it as a channel-open failure turns each restart into a
    /// spike of `unclassified` failures an operator would chase.
    SlotReserved,
}

#[async_trait]
impl ChannelOpener for WedgedOpener {
    async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        _deposit_hint: U256,
        budget: Duration,
    ) -> Result<ChannelContext> {
        if let Ok(mut attempted) = self.attempted.lock() {
            attempted.push(provider_addr);
        }
        if self.wedged.contains(&provider_addr) {
            // Consume exactly the budget, then return the TYPED sentinel — a stand-in
            // for what the real service does once its detached open task has not
            // resolved in time. This is a MODEL of that contract, not a check on it;
            // see the doc above for where the contract itself is guarded.
            //
            // A bare string error would not `downcast_ref::<ChannelOpenPending>()`, so
            // `record_channel_open_failure` would take its generic-failure arm instead
            // of the pending one — the test would still pass (neither arm scores
            // reputation) while never exercising the path it claims to.
            tokio::time::sleep(budget).await;
            return Err(match self.stall {
                OpenStall::Pending => anyhow::Error::new(ChannelOpenPending {
                    provider: provider_addr,
                    waited: budget,
                }),
                OpenStall::SlotReserved => anyhow::Error::new(OpenSlotReserved {
                    provider: provider_addr,
                }),
            });
        }
        Ok(ChannelContext {
            channel_id: self.channel_id,
            token: self.token,
            deposit: self.deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        })
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
        _channel_id: B256,
        _nonce: U256,
        _bytes_delivered: U256,
        _amount: U256,
    ) -> Result<()> {
        Ok(())
    }

    // This opener models a WEDGED open, which never reaches a voucher, so nothing here
    // can ever be rejected and no channel can ever need retiring.
    fn retire_channel(&self, _provider_addr: Address, _channel_id: B256) -> Result<bool> {
        Ok(false)
    }
}

#[async_trait]
impl ChannelOpener for FailingRecordOpener {
    async fn open_or_reuse_channel(
        &self,
        _provider_addr: Address,
        _deposit_hint: U256,
        _budget: Duration,
    ) -> Result<ChannelContext> {
        Ok(ChannelContext {
            channel_id: self.channel_id,
            token: self.token,
            deposit: self.deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        })
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
        _channel_id: B256,
        _nonce: U256,
        _bytes_delivered: U256,
        _amount: U256,
    ) -> Result<()> {
        anyhow::bail!("simulated buyer-channel store write failure")
    }

    // The store is broken in this fixture, so retiring fails the same way a write does.
    fn retire_channel(&self, _provider_addr: Address, _channel_id: B256) -> Result<bool> {
        anyhow::bail!("simulated buyer-channel store write failure")
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
    let resp = ProbeResponse {
        body,
        total_bytes: Some(total_bytes),
        slash_sig,
    };
    let payload = encode_message(&ProbeMessage::Response(resp))
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
                    let _ = handler.accept(conn).await;
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
                    let _ = handler.accept(conn).await;
                });
            }
        }
    })
}

/// Static node-id → region map for the region accountant (#858), mirroring the
/// in-crate `StubResolver` so a test can drive `record_pulled` into an
/// assertable region bucket.
struct StubRegionResolver(HashMap<[u8; 32], String>);

#[async_trait]
impl RegionResolver for StubRegionResolver {
    async fn region_of(&self, node_id: &[u8; 32]) -> Option<String> {
        self.0.get(node_id).cloned()
    }
}

/// A region accountant resolving nothing — every pull buckets into
/// `UNKNOWN_REGION`. Used by the tests that don't assert region totals.
fn empty_region_accountant() -> Arc<RegionAccountant> {
    Arc::new(RegionAccountant::new(Arc::new(StubRegionResolver(
        HashMap::new(),
    ))))
}

/// Build B's `NodeOrigin` with stubbed discovery (`providers` for `hash` via the
/// origin directory), a static `addr_map` resolver, a fixed-channel opener, and
/// real reputation/metrics handles. Tests vary `providers`/`addr_map` to drive
/// the discovery / resolution / probe / pull branches.
///
/// Returns the origin plus the [`StubOpener`]'s `recorded` log so a test can
/// assert what voucher progress was persisted (#852).
#[allow(clippy::too_many_arguments)]
fn provisioned_origin(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    channel_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
) -> (NodeOrigin, Arc<Mutex<Vec<ProgressEntry>>>) {
    provisioned_origin_with_accountant(
        ep_b,
        b_dht,
        hash,
        channel_id,
        buyer_signer,
        local_rep,
        metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
    )
}

/// Like [`provisioned_origin`], but with a caller-supplied region accountant so a
/// test can assert that a delivered pull feeds `bytes_in` (#858).
#[allow(clippy::too_many_arguments)]
fn provisioned_origin_with_accountant(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    channel_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
) -> (NodeOrigin, Arc<Mutex<Vec<ProgressEntry>>>) {
    provisioned_origin_with_deadlines(
        ep_b,
        b_dht,
        hash,
        channel_id,
        buyer_signer,
        local_rep,
        metrics,
        region_accountant,
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES,
    )
}

/// The `(pull_timeout, stall_timeout)` every test that does not care about the deadline gate
/// runs with — generous enough that a loaded runner never trips them.
const DEFAULT_TEST_PULL_DEADLINES: (Duration, Duration) =
    (Duration::from_secs(20), Duration::from_secs(20));

/// Like [`provisioned_origin_with_accountant`], but the caller picks the node-origin
/// deadline budgets.
///
/// The interesting value is a ZERO one. `NodeOriginConfig::deadlines()` refuses it and marks
/// the error `LocalPullFault`, because a zero stall would trip `PullStalled` on the first
/// poll of every read and score `Unreachable` against every honest peer this node touches.
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
fn provisioned_origin_with_deadlines(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    channel_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    (pull_timeout, stall_timeout): (Duration, Duration),
) -> (NodeOrigin, Arc<Mutex<Vec<ProgressEntry>>>) {
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(buyer_signer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        region_accountant,
        providers,
        addr_map,
        pull_timeout,
        stall_timeout,
        0,
    );
    (origin, recorded)
}

/// Like [`provisioned_origin`], but the buyer enforces a `max_blob_size_bytes`
/// ceiling — drives the production `pull_from_candidate` path against the
/// buyer-side gate (#840).
#[allow(clippy::too_many_arguments)]
fn provisioned_origin_with_ceiling(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    channel_id: B256,
    buyer_signer: &Arc<PrivateKeySigner>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    max_blob_size_bytes: u64,
) -> NodeOrigin {
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(buyer_signer),
        voucher_domain: voucher_dom(),
        recorded,
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        max_blob_size_bytes,
    )
}

/// Provision a `NodeOrigin` with stubbed discovery/resolver/reputation around a
/// caller-supplied buyer `ChannelOpener`, so a test can inject any opener
/// (recording, failing, …) without re-wiring the deps.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
fn build_origin(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn ChannelOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
) -> NodeOrigin {
    build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        region_accountant,
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES.0,
        DEFAULT_TEST_PULL_DEADLINES.1,
        0,
    )
}

/// [`build_origin`] with an explicit per-candidate `pull_timeout`, so a test can
/// drive a *short* deadline and exercise the stall-then-fallthrough path (#859)
/// without a 20-second wait.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
fn build_origin_with_timeout(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn ChannelOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
    max_blob_size_bytes: u64,
) -> NodeOrigin {
    build_origin_with_negative_cache(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        region_accountant,
        providers,
        addr_map,
        pull_timeout,
        stall_timeout,
        max_blob_size_bytes,
        NegativeProbeCache::new(),
    )
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
fn build_origin_with_negative_cache(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn ChannelOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
    max_blob_size_bytes: u64,
    negative_cache: NegativeProbeCache,
) -> NodeOrigin {
    build_origin_with_probe_caches(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        metrics,
        region_accountant,
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
}

/// [`build_origin_with_timeout`] with BOTH probe caches injected, so a test can pick each
/// TTL independently.
///
/// The positive cache anchors expiry on `Instant` like its negative twin, so `tokio::time`
/// cannot fast-forward it and 15s per assertion is not a test suite. Injecting the two
/// separately is also what makes their INTERACTION observable: a positive entry that
/// outlives a negative one is how a peer becomes selectable again without a re-probe.
#[allow(clippy::too_many_arguments, clippy::expect_used)]
fn build_origin_with_probe_caches(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    _hash: Hash,
    buyer: Arc<dyn ChannelOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
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
) -> NodeOrigin {
    // Providers are active stakers, matching production (a probe-cache HIT
    // re-checks `is_active`, so an empty set would make every cached provider
    // un-servable on a hit). `find_providers` still returns empty for them — no
    // routing entries — so fetch #1 resolves via the directory under
    // `directory_namespace`. Built before `providers` is moved into `dir`.
    let stakers = ConfigStakerSet::new(providers.iter().copied().collect());
    let mut dir = HashMap::new();
    dir.insert(directory_namespace, providers);

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
        region_accountant: Arc::clone(region_accountant),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout,
            stall_timeout,
            max_blob_size_bytes,
            max_rate_per_mb: 0,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            working_deposit,
            // A day's margin; the fixtures use never-expiring channels, so the
            // near-expiry guard (#1603) is inert unless a test sets an expiry.
            reactive_topup_min_ttl: Duration::from_hours(24),
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    });
    origin
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
fn build_origin_seeded_ranking(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hash: Hash,
    buyer: Arc<dyn ChannelOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    ranked: &[(DhtNodeId, u64)],
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
) -> NodeOrigin {
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
        region_accountant,
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
}

/// Build a `NodeOrigin` whose directory maps SEVERAL hashes to the same provider set, so a
/// test can pull two different blobs from one provider through one shared `deps` — and thus
/// one shared `wedged_providers` map (#1145 review). `max_blob_size_bytes` is 0 (no ceiling).
#[allow(clippy::too_many_arguments, clippy::expect_used)]
fn build_origin_multi_hash(
    ep_b: &iroh::Endpoint,
    b_dht: DhtNodeId,
    hashes: &[Hash],
    buyer: Arc<dyn ChannelOpener>,
    local_rep: &Arc<LocalReputation>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: &[DhtNodeId],
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
) -> NodeOrigin {
    let mut dir = HashMap::new();
    for _h in hashes {
        dir.insert(U256::ZERO, providers.to_vec());
    }
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
        region_accountant: Arc::clone(region_accountant),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout,
            stall_timeout,
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            // Reactive mid-pull top-up OFF (#1530): this fixture asserts what a pull
            // does when its channel runs dry, which a self-funding one would hide.
            working_deposit: U256::ZERO,
            reactive_topup_min_ttl: Duration::from_hours(24),
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    });
    origin
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

/// Current Unix time in microseconds, for a `StreamRequest`'s `timestamp_us` when a test calls
/// a low-level `client-pull` entrypoint directly instead of going through `NodeOrigin` (which
/// generates its own via `node_origin::now_micros`).
fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
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
    let channel_id = B256::repeat_byte(0xB1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

    // Discover → probe → open (with binding) → A reactively pulls its own origin
    // → serve. A held nothing in-store, so a successful pull is proof of the
    // chained reactive fill.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin chained fetch failed: {e}"))?;
    let bytes = fetched.collect_to_bytes().await?.ok_or_else(|| {
        anyhow::anyhow!("node-origin returned NotFound; the chained reactive pull did not fire")
    })?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "chained-pull bytes mismatch"
    );
    anyhow::ensure!(
        cache_a_metrics.origin_fetches.get() == 1,
        "upstream A must reactively pull its origin exactly once, got {}",
        cache_a_metrics.origin_fetches.get()
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// Headline #831 test: a provisioned `NodeOrigin` fills a miss by paid-pulling
/// from an upstream node and records the delivery into the reputation feeds.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_pull_fills_and_records_reputation() -> Result<()> {
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
    let channel_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    // Resolve upstream A to a known region so the delivered pull's inbound bytes
    // land in an assertable bucket (#858).
    let region_accountant = Arc::new(RegionAccountant::new(Arc::new(StubRegionResolver(
        HashMap::from([(*a_id.as_bytes(), "DE".to_string())]),
    ))));
    let (origin, recorded) = provisioned_origin_with_accountant(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        &region_accountant,
        providers,
        addr_map,
    );

    // --- The orchestration: discover → probe → rank → open → pull. ------------
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    let bytes = fetched
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("node-origin returned NotFound; expected the blob"))?;
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
            == vec![(
                a_eth.address(),
                U256::from(2),
                U256::from(expected_wire),
                expected_amount
            )],
        "expected one persisted progress entry with the final voucher totals, got {:?}",
        progress_log(&recorded)?
    );

    // #858: the delivered pull fed the region accountant's inbound counter for
    // upstream A's region — the gap that left `bytes_in` stuck at 0.
    anyhow::ensure!(
        region_accountant.snapshot()
            == vec![RegionBytes {
                region: "DE".to_string(),
                bytes_in: total_bytes,
                bytes_out: 0,
            }],
        "expected DE bytes_in == {total_bytes}, got {:?}",
        region_accountant.snapshot()
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    for chunk in served.chunks(CHUNK_SIZE) {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    // Ack the closing voucher so the requester proceeds to the integrity check.
    let voucher_msg = {
        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
        decode_message::<ClientMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode voucher: {e}"))?
            .0
    };
    if let ClientMessage::Voucher(_) = voucher_msg {
        write_frame(&mut send, &encode_message(&ClientMessage::VoucherAck)?)
            .await
            .map_err(|e| anyhow::anyhow!("write ack: {e}"))?;
    }
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
    // The DECOUPLED serve-miss (#1621 B2 part 2) reuses ONE upstream connection for
    // TWO bi-streams: first a FREE header handshake — a whole-tail open
    // (`byte_offset == 0 && byte_len == 0`) that the buyer ABORTS right after reading
    // `total_bytes`, so it pulls no chunk, pays no voucher, and records NO watermark —
    // then the real `PeerSource` range pull (`byte_len > 0`) on a SECOND bi-stream of
    // the same connection. The fused path did a single open; modelling that here (one
    // `accept_bi`, one gate) made the free handshake swallow the test's single
    // `release`, so the real pull blocked forever → 20s stall → `early eof`.
    //
    // Fix: loop over the connection's bi-streams. Gate ONLY the real pull — answer the
    // handshake immediately (never touching `release`) and never signal `received` for
    // it, so the test's `received` wait resolves on the real owner pull (the in-flight
    // tee) landing, and the single `release` reaches the pull that actually blocks on
    // it. The handshake records no watermark, so `upstream.len() == 1` (single SPEND)
    // still holds.
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
        if req.byte_offset == 0 && req.byte_len == 0 {
            // Free header handshake: answer without gating, then loop back to accept
            // the real pull's bi-stream. The buyer aborts after the header, so there
            // is no voucher exchange to await.
            write_frame(
                &mut send,
                &encode_message(&ClientMessage::StreamResponse(resp))?,
            )
            .await
            .map_err(|e| anyhow::anyhow!("write handshake response: {e}"))?;
            let _ = send.finish();
            continue;
        }
        // The real range pull landed (B's claim_fill Owner pull is in flight, cache still empty);
        // hold here until the test has opened the coalescing second request.
        received.notify_one();
        release.notified().await;
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::StreamResponse(resp))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
        for chunk in served.chunks(CHUNK_SIZE) {
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
        write_frame(&mut send, &encode_message(&ClientMessage::VoucherAck)?)
            .await
            .map_err(|e| anyhow::anyhow!("write ack: {e}"))?;
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
    use iroh::protocol::ProtocolHandler;
    tokio::spawn(async move {
        while let Some(incoming) = server_ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let _ = handler.accept(conn).await;
            });
        }
    })
}

/// A protocol-correct upstream that serves the *right* bytes but rejects the
/// closing voucher with `StreamError(VoucherRejected { StaleNonce })` instead of
/// `VoucherAck` — the buyer-side payment failure of #857/#852. Drives the
/// requester to `UpstreamVoucherRejected`. Modelled on [`serve_wrong_bytes`] but
/// serving the correct payload so the failure is unambiguously the voucher leg,
/// not corruption.
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
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    for chunk in served.chunks(CHUNK_SIZE) {
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
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    for chunk in served.chunks(CHUNK_SIZE) {
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
/// channel wedges, however the candidate list that would have re-selected it was built.
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
/// so the buyer's `stream_fetch` errors on connect/read rather than stalling to a
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
/// `StreamResponse` and holds the send side open, so the buyer's `stream_fetch`
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
    let channel_id = B256::repeat_byte(0xA2);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    // Map both candidates to distinct regions so the snapshot proves the stalled
    // candidate (Err arm) records no `bytes_in` while only the delivered honest
    // fallback (Ok arm) is counted (#858).
    let region_accountant = Arc::new(RegionAccountant::new(Arc::new(StubRegionResolver(
        HashMap::from([
            (*s_id.as_bytes(), "XX".to_string()),
            (*a_id.as_bytes(), "DE".to_string()),
        ]),
    ))));
    // The staller quotes the cheaper `STALL_RATE` so it ranks ahead of A (`RATE`).
    // Order is PINNED via a seeded probe cache: RTT is a multiplicative ranker term, so
    // a loaded runner's live-probe jitter could otherwise float A ahead of the staller,
    // deliver from A first, and leave the staller untried (no timeout, flake). See
    // `build_origin_seeded_ranking`.
    let origin = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &region_accountant,
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
    );

    // The orchestration must abandon the staller and deliver from A.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    let bytes = fetched
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected the blob from the honest fallback candidate"))?;
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

    // #858: the stalled candidate S (Err arm) records no `bytes_in`; only the
    // delivered honest fallback A (Ok arm, region "DE") is counted — no "XX"
    // bucket appears. Guards the "failed pulls are not counted" contract and
    // multi-candidate attribution in one assertion.
    anyhow::ensure!(
        region_accountant.snapshot()
            == vec![RegionBytes {
                region: "DE".to_string(),
                bytes_in: total_bytes,
                bytes_out: 0,
            }],
        "expected only DE bytes_in == {total_bytes} (S exonerated, not counted), got {:?}",
        region_accountant.snapshot()
    );

    ep_b.close().await;
    ep_a.close().await;
    ep_s.close().await;
    task_a.await?;
    task_s.await?;
    Ok(())
}

/// A wedged CHANNEL OPEN must not starve the candidate fallback loop (#1143).
///
/// This is the stage #1141/#1142 did *not* bound. Those fixed the stall once a
/// candidate accepts a QUIC connection; `open_or_reuse_channel` runs BEFORE that,
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
/// Run against BOTH stalls (#1145 review). They are handled by different arms of
/// `record_channel_open_failure` and mean different things — one says our chain lane is
/// slow, the other that a reconcile is re-hydrating the row — but they must produce the
/// SAME outcome here: the loop moves on, the peer is not scored, and neither counts as a
/// channel-open FAILURE. Only the `Pending` arm was ever exercised; deleting the
/// `OpenSlotReserved` arm outright left 749/749 green, even though it fires at every boot.
// multi-node fixture setup, like its siblings above; the two wedged nodes are
// deliberately named in parallel (`w_*` / `w2_*`) so the pair reads as a pair.
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn wedged_open_does_not_starve_the_candidate_loop(stall: OpenStall) -> Result<()> {
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
    let channel_id = B256::repeat_byte(0xE1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        attempted: Arc::clone(&attempted),
        stall,
    }) as Arc<dyn ChannelOpener>;
    let region_accountant = Arc::new(RegionAccountant::new(Arc::new(StubRegionResolver(
        HashMap::from([
            (*w_id.as_bytes(), "XX".to_string()),
            (*w2_id.as_bytes(), "XX".to_string()),
            (*a_id.as_bytes(), "DE".to_string()),
        ]),
    ))));
    // Generous: this fixture wedges at the CHANNEL-OPEN stage, so the streaming
    // inactivity bound must never be what ends a candidate here. It still has to be a
    // real value — it is a term of the outer deadline.
    let stall_budget = Duration::from_secs(20);
    let per_candidate = Duration::from_secs(2);
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &region_accountant,
        vec![w_dht, w2_dht, a_dht],
        addr_map,
        per_candidate,
        stall_budget,
        0,
    );

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
    let bytes = fetched
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected the blob from the honest fallback candidate"))?;
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
    // may take a reputation hit, locally or over gossip. This is the same exoneration
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
    // Both wedged candidates took the PENDING arm of `record_channel_open_failure`,
    // not the generic channel-open-failure arm. Both arms record no reputation, so
    // without these two counters the assertions above would pass even if the typed
    // `ChannelOpenPending` were never produced — the test would prove nothing about
    // the mechanism it exists for. The split also matters operationally: "pending"
    // says the node's chain lane is slower than `CHANNEL_OPEN_CALLER_BUDGET`, while
    // "failure" says the tx reverted or the wallet is under-funded.
    assert_counter(&b_metrics, "node_pull_channel_open_pending_total", 2)?;
    assert_counter(&b_metrics, "node_pull_channel_open_failures_total", 0)?;

    ep_b.close().await;
    ep_a.close().await;
    ep_w.close().await;
    ep_w2.close().await;
    task_a.await?;
    task_w.await?;
    task_w2.await?;
    Ok(())
}

/// The open outlived the caller's budget and continues in a detached task (#1143).
#[tokio::test]
async fn a_wedged_channel_open_does_not_starve_the_candidate_loop() -> Result<()> {
    wedged_open_does_not_starve_the_candidate_loop(OpenStall::Pending).await
}

/// A reconcile holds the provider's open slot and tells us to retry (#1145 review).
///
/// Its own arm, and an unexercised one until now. It is not interchangeable with the
/// pending arm even though both end in the same counter: reaching the generic
/// channel-open-FAILURE arm instead would turn every single boot — when the reconcile scan
/// runs — into a spike of `unclassified` failures for an operator to chase.
#[tokio::test]
async fn a_reserved_open_slot_does_not_starve_the_candidate_loop() -> Result<()> {
    wedged_open_does_not_starve_the_candidate_loop(OpenStall::SlotReserved).await
}

/// The WINDOW-path counterpart of `node_origin_pull_falls_through_a_stalled_candidate`
/// (#856, #1141). The buffered path bounds each candidate with `pull_timeout` inside
/// `stream_fetch_tracked`; `open_progressive_pull` must do the same, or the first
/// candidate to accept the connection and go quiet consumes the caller's ENTIRE
/// budget and candidates #2..N are never opened — the serve path then refuses a
/// blob the honest fallback holds.
///
/// TWO stallers, not one, and that is the point: with a single staller a
/// regression that applied the budget to the whole LOOP rather than per-candidate
/// still passes (one 3s stall fits under any plausible outer bound). Two stalls
/// only fit if the budget is genuinely per-candidate — which is exactly the
/// property #859 sized the outer deadline around and #1141 broke on this path.
///
/// The 12s wrapper stands in for the serve path's outer deadline (the real
/// `selection::outer_pull_deadline` for a 3s per-candidate budget is
/// 3×(5s channel-open + 3s) + 10s slack = 34s; 12s is a tighter stand-in that still
/// admits two 3s stalls plus a healthy open — the opens here are instant, so no
/// candidate spends its channel-open budget). With no per-candidate bound, staller
/// #1 eats the whole wrapper and this elapses.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)]
async fn node_origin_window_open_falls_through_a_stalled_candidate() -> Result<()> {
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
    let channel_id = B256::repeat_byte(0xA3);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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

    // --- Stalling nodes S1, S2: both quote the CHEAPER rate so they rank ahead of
    //     A, then accept the client stream and never answer. Two of them, so the
    //     budget must be per-candidate to reach A at all. ----------------------
    let s1_sk = fresh_key();
    let s1_id = s1_sk.public();
    let s1_eth = Arc::new(PrivateKeySigner::random());
    let (ep_s1, addr_s1) =
        local_endpoint(s1_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_s1 = spawn_a_stalling_server(
        ep_s1.clone(),
        Arc::clone(&s1_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
    );

    let s2_sk = fresh_key();
    let s2_id = s2_sk.public();
    let s2_eth = Arc::new(PrivateKeySigner::random());
    let (ep_s2, addr_s2) =
        local_endpoint(s2_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_s2 = spawn_a_stalling_server(
        ep_s2.clone(),
        Arc::clone(&s2_eth),
        slash_domain(),
        total_bytes,
        STALL_RATE,
    );

    // --- Node B: dial-only endpoint hosting the NodeOrigin. -------------------
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    for (id, addr) in [(a_id, addr_a), (s1_id, addr_s1), (s2_id, addr_s2)] {
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

    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let region_accountant = Arc::new(RegionAccountant::new(Arc::new(StubRegionResolver(
        HashMap::from([
            (*s1_id.as_bytes(), "XX".to_string()),
            (*s2_id.as_bytes(), "XX".to_string()),
            (*a_id.as_bytes(), "DE".to_string()),
        ]),
    ))));
    // 3s per candidate. Deliberately not 1s: this budget now bounds EVERY candidate
    // open, including the honest one, and A's open is a real QUIC handshake +
    // `open_or_reuse_channel` + verified-header exchange. At 1s a loaded CI runner
    // could abandon A too and fail the test for an unrelated reason.
    let per_candidate = Duration::from_secs(3);
    // Both stallers quote the cheaper `STALL_RATE` so they rank strictly ahead of A
    // (`RATE`). Order is PINNED via a seeded probe cache rather than a live probe: RTT
    // is a multiplicative term in the ranker, so a loaded runner's probe jitter could
    // otherwise float A ahead of a staller and leave it untried (only one timeout, flake).
    // See `build_origin_seeded_ranking`.
    let origin = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &region_accountant,
        &[(s1_dht, STALL_RATE), (s2_dht, STALL_RATE), (a_dht, RATE)],
        addr_map,
        per_candidate,
        Duration::from_secs(20),
    );

    // The open loop must abandon BOTH stallers on their own budgets and open A.
    let opened = tokio::time::timeout(
        Duration::from_secs(12),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "open_progressive_pull never returned: a stalled candidate consumed the whole \
                 outer budget, so the honest fallback was never opened"
        )
    })?;
    let (header, _pull) = opened.map_err(|miss| {
        anyhow::anyhow!("expected an open against the honest fallback candidate, got {miss:?}")
    })?;
    // A quotes `RATE`; the stallers quote the cheaper `STALL_RATE` (which is why they
    // rank ahead of it). The rate on the verified header therefore proves WHICH
    // provider we opened against.
    anyhow::ensure!(
        header.rate_per_mb == RATE,
        "expected the open against the honest fallback A (rate {RATE}), got rate {}",
        header.rate_per_mb
    );
    anyhow::ensure!(
        header.total_bytes == total_bytes,
        "unexpected total_bytes {}",
        header.total_bytes
    );

    // Each staller hit OUR per-candidate deadline — a buyer-side budget, not evidence
    // the provider is bad — so both are exonerated: no local EWMA hit, nothing
    // gossiped (#857). Identical to the buffered path's contract.
    for (label, s_id) in [("S1", s1_id), ("S2", s2_id)] {
        anyhow::ensure!(
            (local_rep.score(s_id) - 0.5).abs() < f64::EPSILON,
            "the timed-out staller {label}'s local score must stay neutral, got {}",
            local_rep.score(s_id)
        );
    }
    // TWO timeouts — the load-bearing assertion. A budget applied to the whole loop
    // instead of per-candidate would abandon after the first and never reach A.
    assert_counter(&b_metrics, "node_pull_timeout_total", 2)?;
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 0)?;

    // The abandoned opens forfeited nothing: no voucher was signed for either staller
    // (the open dies before the first chunk), and no bytes were attributed to their
    // region. Guards the resource half of #1141 — a per-candidate timeout that leaked
    // spend on every stall would be a poor trade.
    let ledger_len = {
        let ledger = recorded.lock().expect("progress ledger not poisoned");
        ledger.len()
    };
    anyhow::ensure!(
        ledger_len == 0,
        "a stalled open must record no voucher progress, got {ledger_len} entries"
    );
    anyhow::ensure!(
        region_accountant.snapshot().is_empty(),
        "nothing was delivered, so no region should have bytes_in; got {:?}",
        region_accountant.snapshot()
    );

    ep_b.close().await;
    ep_a.close().await;
    ep_s1.close().await;
    ep_s2.close().await;
    task_a.await?;
    task_s1.await?;
    task_s2.await?;
    Ok(())
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
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &b_metrics,
        Vec::new(),
        HashMap::new(),
    );

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
    ep_b.close().await;
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
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &b_metrics,
        vec![DhtNodeId::from_bytes(*a_id.as_bytes())],
        HashMap::new(),
    );

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
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &b_metrics,
        vec![DhtNodeId::from_bytes(*a_id.as_bytes())],
        addr_map,
    );

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
    ep_b.close().await;
    Ok(())
}

/// A dishonest upstream (valid response, wrong bytes) is detected at the
/// whole-blob integrity check and scored `Corruption` (`data_correct`: false),
/// the local score drops, the corruption counter moves, and no bytes surface.
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
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

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
    // interval ⇒ one closing voucher: nonce 1, 4096 bytes, amount 1.
    anyhow::ensure!(
        progress_log(&recorded)?
            == vec![(
                a_eth.address(),
                U256::from(1),
                U256::from(4096),
                U256::from(1)
            )],
        "corrupt-but-paid delivery must persist its acked watermark, got {:?}",
        progress_log(&recorded)?
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok(())
}

/// #857 over the real orchestration: an upstream serves the *correct* bytes but
/// rejects the buyer's closing voucher (a stale nonce / our payment fault). The
/// pull must fail to a clean `NotFound`, and — crucially — the provider must NOT
/// be tarred: a voucher rejection is OUR payment-side fault, so no observation is
/// emitted (local or gossiped), the local score stays neutral, and only the
/// buyer-side `node_pull_voucher_rejected` counter moves (no unreachable, no
/// corruption). Before the fix, this self-inflicted failure mapped to
/// `Outcome::Unreachable` and defamed the honest provider network-wide.
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
        VoucherRejectReason::StaleNonce,
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
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    let channel_id = B256::repeat_byte(0xC7);

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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::clone(&retired),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok((retired, b_metrics, local_rep, a_id, channel_id))
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

/// A channel that can no longer pay must KEEP its row — the deposit is still escrowed, and
/// the row is the only thing that can ever reclaim it (#1145 review).
///
/// This test previously asserted the exact opposite, and that is the point of rewriting it
/// rather than adapting it: it pinned a money-losing behaviour. It required
/// `InsufficientDeposit` to `retire_channel` the row, on the stated grounds that "the
/// on-chain deposit stays escrowed and is recovered by the ordinary settlement sweep /
/// `reclaimExpired`, exactly as for any other channel we stop using."
///
/// That premise is false, and the codebase already knew it. `reclaimExpired` refunds the
/// remainder and needs the `channel_id`; BOTH recovery sweeps find their work by
/// enumerating the store (`load_all`); and the boot reconcile's lookback is ~1–2 days
/// against a ~90-day expiry. A forgotten row is therefore a deposit nothing can ever see
/// again — which is precisely why `run_open` reclaims BEFORE it rotates an expired channel,
/// warning in its own comment that otherwise the sweep "would never see it again — silently
/// abandoning a refundable deposit".
///
/// Nor is the money gone in the first place: `InsufficientDeposit` means too little for THIS
/// voucher, not nothing left. So the row survives, the provider is suppressed instead, and
/// the deposit is reclaimed at expiry.
///
/// What was already right is preserved: the provider is still not tarred — a drained deposit
/// is our fault, not its.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_a_drained_channel_keeps_its_row_so_the_deposit_can_be_reclaimed() -> Result<()>
{
    // Two fetches: the second is how suppression is observed. A wedged provider must be
    // filtered out of ranking, so the second miss must not re-present a voucher to it.
    let (retired, metrics, local_rep, a_id, _channel_id) =
        pull_against_a_voucher_rejecting_upstream_n(VoucherRejectReason::InsufficientDeposit, 2)
            .await?;

    let log = retired
        .lock()
        .map_err(|_| anyhow::anyhow!("retired lock poisoned"))?
        .clone();
    anyhow::ensure!(
        log.is_empty(),
        "the row must SURVIVE: its deposit is still escrowed, and `reclaimExpired` needs the \
         channel_id that only this row carries. Deleting it strands the deposit forever — no \
         sweep enumerates a row that is gone. Got {log:?}"
    );

    // The channel is wedged, not retired — and the two are different events with different
    // remedies, so they get different series.
    assert_counter(&metrics, "node_pull_channel_wedged_total", 1)?;
    assert_counter(&metrics, "node_pull_channel_retired_total", 0)?;

    // Suppression, observed rather than asserted about: the provider was taken out of
    // rotation, so the SECOND fetch never reached it. Without it, the wedged channel is
    // handed straight back (`try_reuse_live` gates on expiry alone) and we re-present a
    // voucher it cannot honour on every miss until it expires.
    assert_counter(&metrics, "node_pull_voucher_rejected_total", 1)?;

    // Still OUR fault, not the provider's: the exoneration that was already correct must
    // survive the fix.
    assert_counter(&metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "provider score must stay neutral, got {}",
        local_rep.score(a_id)
    );
    Ok(())
}

/// The one rejection for which dropping the row IS correct: the upstream holds a signed
/// cooperative close, so the channel is settled on-chain and there is no remainder to
/// reclaim (#1145 review).
///
/// The negative twin of the test above, and the reason the verdict had to be split in two
/// rather than made uniformly "keep the row". A settled channel kept in the store would be
/// handed straight back by `try_reuse_live` (which gates on expiry alone) and rejected on
/// every subsequent miss, for nothing — there is no deposit left for the row to protect.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_a_cooperatively_closed_channel_is_the_one_that_may_be_forgotten() -> Result<()>
{
    let (retired, metrics, local_rep, a_id, channel_id) =
        pull_against_a_voucher_rejecting_upstream(VoucherRejectReason::CooperativeCloseSigned)
            .await?;

    let log = retired
        .lock()
        .map_err(|_| anyhow::anyhow!("retired lock poisoned"))?
        .clone();
    anyhow::ensure!(
        log.len() == 1,
        "a settled channel must be retired exactly once so the next pull opens a fresh one; \
         got {log:?}"
    );
    anyhow::ensure!(
        log.first().map(|(_, id)| *id) == Some(channel_id),
        "retire must name the channel the pull actually paid on — a compare-and-delete on any \
         other id would throw away a channel a concurrent open had just created"
    );

    assert_counter(&metrics, "node_pull_channel_retired_total", 1)?;
    assert_counter(&metrics, "node_pull_channel_wedged_total", 0)?;
    assert_counter(&metrics, "node_pull_voucher_rejected_total", 1)?;
    assert_counter(&metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "provider score must stay neutral, got {}",
        local_rep.score(a_id)
    );
    Ok(())
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

/// #1560 on the WINDOW path: a local fault must leave the open reporting
/// `PullMiss::LocalFault`, not the clean miss that the serve path signs as a wire
/// `NotFound`.
///
/// The fault is induced through the deadline gate — a zero `stall_timeout`, which
/// `NodeOriginConfig::deadlines()` marks `LocalPullFault` because a zero stall trips
/// `PullStalled` on the first poll and would score `Unreachable` against every honest peer
/// this node touches. That is a real, documented local fault ("check
/// `cache.node_pull_timeout_sec`"), and unlike a forged broken signer it is deterministic and
/// never reaches the wire: the pull resolves the candidate, opens its channel, signs the
/// binding, and only then finds it has no legal budget to run under. So there is no server
/// here at all, and no way for a network condition to be mistaken for the fault under test.
///
/// Seeded with exactly [`MAX_PROVIDER_ATTEMPTS`] candidates, which is load-bearing: the walk
/// then spends the whole fetch-wide budget and leaves through the probe-cache EXHAUSTION
/// exit rather than falling through to a fresh lookup. That is the second of the two arms
/// this fix had to cover — its own comment used to defer to the cold arm's KNOWN LIMITATION
/// note — and it is also what keeps the test hermetic: a cold fallthrough would live-probe
/// providers that do not exist and score them `Unreachable`, which has nothing to do with
/// the fault under test.
///
/// The counter assertion is not redundant with the verdict. Metering and answering are
/// different obligations and the bug was precisely that the first held while the second did
/// not — `node_pull_local_fault_total` moved, "any sustained rate is an emergency", and the
/// client was told the blob does not exist.
#[tokio::test(flavor = "multi_thread")]
async fn window_open_reports_a_local_fault_rather_than_a_clean_miss() -> Result<()> {
    let payload = vec![0x5Au8; 4096];
    let hash = Hash::new(&payload);

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let mut addr_map = HashMap::new();
    let ranked: Vec<(DhtNodeId, u64)> = (0..MAX_PROVIDER_ATTEMPTS)
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
        channel_id: B256::repeat_byte(0x5A),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        &ranked,
        addr_map,
        Duration::from_secs(20),
        // The fault: no stall budget means no pull may legally run.
        Duration::ZERO,
    );

    let miss = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("open_progressive_pull never returned"))?
    .err()
    .ok_or_else(|| anyhow::anyhow!("a pull with no legal deadline budget cannot open"))?;
    anyhow::ensure!(
        miss.is_local_fault(),
        "an unusable deadline config is OUR fault; reporting it as a clean miss is what \
         signs a client `NotFound` for content this node never even asked for, got {miss:?}"
    );
    assert_counter(
        &b_metrics,
        "node_pull_local_fault_total",
        u64::try_from(MAX_PROVIDER_ATTEMPTS).unwrap_or(u64::MAX),
    )?;
    // No peer is touched: none was ever dialed, and a fault of ours must never be
    // spent on anyone's reputation.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;

    ep_b.close().await;
    Ok(())
}

/// How a [`FailingOpener`]'s channel open fails — one per class the caller-side ladder
/// distinguishes (#1560).
///
/// **Each shape reproduces the marker chain its production counterpart actually carries**,
/// which is the whole point of the enum. The first cut of this fixture attached only the
/// distinguishing marker, and that omission hid the bug it was written to catch: every
/// production leg of the open task also carries `OpenReported`, so a `NodeWide` error with
/// no `OpenReported` cannot detect whether the ladder checks `LocalPullFault` first, and a
/// `PerProvider` error with no `OpenReported` was answered `Clean` by an arm that no real
/// error ever reaches. Both mutations stayed green. Keep the chains faithful.
#[derive(Debug, Clone, Copy)]
enum OpenFailureShape {
    /// Typed `LocalPullFault` by the buyer path itself — `join_or_spawn_open`'s
    /// poisoned-`opens_in_flight` leg (which logs that this node "can no longer open a
    /// buyer channel to ANY provider and must be restarted"), `reuse_live_or_report`'s
    /// unreadable-store leg (recoverable: "until the store recovers"), `run_open`'s store
    /// write, a panicked open task, and an `InsufficientDeposit` wallet. All of them ALSO
    /// carry `OpenReported`, because the open path reports before it returns.
    NodeWide,
    /// A classified, per-provider on-chain condition, reported by the open task like every
    /// other leg. Another candidate may still pay.
    PerProvider,
    /// Unclassified and raised outside the open task — the residual, which the ladder reads
    /// as this node's own state. No `OpenReported`, because nothing reported it.
    Residual,
}

/// A [`ChannelOpener`] that always fails, in a caller-selected shape.
#[derive(Debug)]
struct FailingOpener {
    shape: OpenFailureShape,
}

#[async_trait]
impl ChannelOpener for FailingOpener {
    async fn open_or_reuse_channel(
        &self,
        _provider_addr: Address,
        _deposit_hint: U256,
        _budget: Duration,
    ) -> Result<ChannelContext> {
        let err = anyhow::anyhow!("stub channel open failed");
        Err(match self.shape {
            OpenFailureShape::NodeWide => err.context(OpenReported).context(LocalPullFault),
            OpenFailureShape::PerProvider => err
                .context(ChannelOpenFailureReason::ContractRevert)
                .context(OpenReported),
            OpenFailureShape::Residual => err,
        })
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
        _channel_id: B256,
        _nonce: U256,
        _bytes_delivered: U256,
        _amount: U256,
    ) -> Result<()> {
        Ok(())
    }

    fn retire_channel(&self, _provider_addr: Address, _channel_id: B256) -> Result<bool> {
        Ok(false)
    }
}

/// A channel open that fails because THIS node's buyer side is broken must not be answered
/// as an absent blob (#1566 review).
///
/// This is the leg the first cut of #1560 missed. It upgraded `classify_pull_failure` to
/// return a verdict, but `record_channel_open_failure` stayed purely side-effecting, so both
/// call sites hardcoded a clean miss — and the two loudest node-wide buyer faults in the
/// crate land exactly there. `join_or_spawn_open` logs "this node can no longer open a buyer
/// channel to ANY provider and must be restarted" and then, before this fix, told every
/// client the content did not exist.
///
/// Attribution comes from a marker the raising site attaches, and all three classes are
/// driven here with production's real marker chains. A `ContractRevert` reported by the open
/// task is one provider's on-chain condition and must stay a clean miss, or a single unlucky
/// provider would make a healthy node declare itself broken. An unmarked residual counts as
/// ours, because every leg the open path knows about returns earlier — so what is left was
/// raised outside it and nobody classified it.
///
/// The expectation is written as a literal per shape rather than derived from a helper: a
/// symmetric `is_local_fault() == shape.is_ours()` also passes when BOTH sides invert, which
/// is precisely the mutation a classifier refactor would produce.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_wide_channel_open_fault_refuses_rather_than_reporting_an_absent_blob() -> Result<()>
{
    // (shape, is a fault of ours, bumps the generic channel-open-failure counter)
    for (shape, expect_ours, expect_open_failure_counter) in [
        (OpenFailureShape::NodeWide, true, false),
        (OpenFailureShape::PerProvider, false, false),
        (OpenFailureShape::Residual, true, true),
    ] {
        let payload = vec![0x5Eu8; 4096];
        let hash = Hash::new(&payload);

        let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
        let b_id = fresh_key().public();
        // Exactly `MAX_PROVIDER_ATTEMPTS`, so the walk spends the whole budget and leaves
        // through the exhaustion short-circuit. A shorter list would fall through to a
        // fresh lookup that live-probes providers which do not exist and scores them
        // `Unreachable` — noise from a stage this test is not about.
        let mut addr_map = HashMap::new();
        let ranked: Vec<(DhtNodeId, u64)> = (0..MAX_PROVIDER_ATTEMPTS)
            .map(|_| {
                let dht = DhtNodeId::from_bytes(*fresh_key().public().as_bytes());
                addr_map.insert(dht, PrivateKeySigner::random().address());
                (dht, RATE)
            })
            .collect();

        let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
        let b_metrics = Arc::new(Metrics::new());
        let origin = build_origin_seeded_ranking(
            &ep_b,
            DhtNodeId::from_bytes(*b_id.as_bytes()),
            hash,
            Arc::new(FailingOpener { shape }) as Arc<dyn ChannelOpener>,
            &local_rep,
            &b_metrics,
            &empty_region_accountant(),
            &ranked,
            addr_map,
            DEFAULT_TEST_PULL_DEADLINES.0,
            DEFAULT_TEST_PULL_DEADLINES.1,
        );

        let miss = tokio::time::timeout(
            Duration::from_secs(10),
            origin.open_progressive_pull(hash, U256::ZERO),
        )
        .await
        .map_err(|_| anyhow::anyhow!("open_progressive_pull never returned"))?
        .err()
        .ok_or_else(|| anyhow::anyhow!("no channel can be opened, so no pull can start"))?;

        anyhow::ensure!(
            miss.is_local_fault() == expect_ours,
            "a {shape:?} channel-open failure must report is_local_fault()={expect_ours}, \
             got {miss:?}"
        );
        let attempts = u64::try_from(MAX_PROVIDER_ATTEMPTS).unwrap_or(u64::MAX);
        assert_counter(
            &b_metrics,
            "node_pull_local_fault_total",
            if expect_ours { attempts } else { 0 },
        )?;
        // Distinguishes the three arms from each other, not just their verdicts. The
        // `LocalPullFault` arm returns before the generic counter is bumped and the
        // `OpenReported` arm never reaches it, so only the residual moves this one — which
        // means deleting either of the first two arms changes this assertion too.
        assert_counter(
            &b_metrics,
            "node_pull_channel_open_failures_total",
            if expect_open_failure_counter {
                attempts
            } else {
                0
            },
        )?;
        // Every provider is exonerated regardless: none of them ever got a request.
        assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;

        ep_b.close().await;
    }
    Ok(())
}

/// The BUFFERED twin of the test above, on the exit that is likelier to fire in production.
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
        channel_id: B256::repeat_byte(0x5D),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        &ranked,
        addr_map,
        Duration::from_secs(20),
        Duration::ZERO,
    );

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

    ep_b.close().await;
    Ok(())
}

/// The control for the test above, and the half that must NOT change: an upstream that
/// honestly refuses is still a clean miss.
///
/// Without it, "report every failed open as a local fault" passes the test above and breaks
/// the property `StreamError::NotFound` exists for — a healthy-but-empty node answering
/// truthfully. `InternalError` means "do not retry THIS node" (#1129), so mislabelling an
/// honest miss steers clients off a node that is working perfectly.
#[tokio::test(flavor = "multi_thread")]
async fn window_open_still_reports_an_honest_refusal_as_a_clean_miss() -> Result<()> {
    let payload = vec![0x5Bu8; 4096];
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
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0x5B),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

    let miss = tokio::time::timeout(
        Duration::from_secs(20),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("open_progressive_pull never returned"))?
    .err()
    .ok_or_else(|| anyhow::anyhow!("a refused open cannot yield a pull"))?;
    anyhow::ensure!(
        !miss.is_local_fault(),
        "a peer that truthfully refuses is not evidence THIS node is broken; calling it a \
         local fault refuses the client `InternalError` and steers it off a healthy node"
    );
    assert_counter(&b_metrics, "node_pull_local_fault_total", 0)?;
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    let channel_id = B256::repeat_byte(0x5C);
    let (cache_a, hash_a, tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    std::mem::forget(tmp_a);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_seeded_ranking(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        &[(f_dht, CHEAP_RATE), (a_dht, RATE)],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
    );

    let got = tokio::time::timeout(
        Duration::from_secs(20),
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch never returned"))?
    .map_err(|e| anyhow::anyhow!("a walk that reached an honest candidate must deliver: {e}"))?;
    let delivered = got
        .collect_to_bytes()
        .await
        .map_err(|e| anyhow::anyhow!("drain the delivered stream: {e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("the honest fallback held the blob; the walk must not miss")
        })?;
    anyhow::ensure!(
        delivered.as_ref() == payload.as_slice(),
        "the fallback candidate's bytes must be the ones returned"
    );
    // Both halves: the fault was seen and metered, and it did not become the answer.
    assert_counter(&b_metrics, "node_pull_local_fault_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;

    ep_b.close().await;
    ep_a.close().await;
    ep_f.close().await;
    task_a.await?;
    task_f.await?;
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
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// #1088 / #1134 / #1144 — the wire fixtures whose failure shapes the four
// bounds/classification fixes in this PR exist for. Each of the tests below
// FAILS if its production change is reverted; the fixtures are the reason.
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
) -> Result<StreamResponse> {
    let body = StreamResponseBody {
        hash: req.hash,
        ok: error.is_none(),
        rate_per_mb: rate,
        total_bytes,
        channel_id: req.channel_id,
        timestamp_us: req.timestamp_us,
        redirect: None,
    };
    let slash_sig = StreamSlashData::from_response_body(&body)
        .sign(eth.as_ref(), slash)
        .map_err(|e| anyhow::anyhow!("slash sign: {e}"))?
        .as_bytes()
        .to_vec();
    Ok(StreamResponse {
        body,
        error,
        voucher_interval_mb: Some(1),
        slash_sig,
    })
}

/// The honest whole-blob bao verified-stream wire for `payload` (ADR 038), with
/// the 8-byte LE size header stripped — exactly the byte sequence an honest
/// upstream emits on `cdn/client/v1`. Sibling of the corrupt wire built inline by
/// `window_pull_through_mid_stream_corruption_scores_upstream_not_local`.
fn honest_bao_wire(payload: &[u8]) -> Result<Vec<u8>> {
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

/// A protocol-correct upstream that accepts the request, signs a valid
/// `StreamResponse`, and then emits an UNBOUNDED run of EMPTY `ChunkData` frames
/// (#1088).
///
/// This is the hostile shape the non-empty floor exists for, and it is bounded by
/// nothing else on either receive loop:
///
/// - the `CHUNK_SIZE` ceiling passes trivially (0 ≤ ceiling),
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
    let resp = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
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
/// `prefix_chunks` frames are sent (under the 1 MiB voucher interval, so no
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
    let resp = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
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

/// The first `chunks` `ChunkData` frames of `wire`, each `CHUNK_SIZE` bytes (the
/// last one short if `wire` runs out).
///
/// Shared by the go-silent fixtures so a "prefix" is always a genuine prefix of the
/// blob's bao encoding rather than filler that the buyer's incremental decoder would
/// reject as corruption before the fixture's real behaviour ever ran.
fn wire_frames(wire: &[u8], chunks: usize) -> Result<Vec<Vec<u8>>> {
    wire.chunks(CHUNK_SIZE)
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

/// A provider that opens honestly, delivers a FULL voucher interval, acks the
/// voucher the buyer presents for it — and only THEN goes silent (#1145 review).
///
/// The distinction from [`serve_then_go_silent`] is the whole point. That one stays
/// deliberately UNDER the 1 MiB voucher interval, so no voucher round trip intrudes:
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
    let resp = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;

    // Exactly one voucher interval: `signed_response` advertises
    // `voucher_interval_mb: Some(1)`, and CHUNK_SIZE divides 1 MiB evenly, so this
    // lands the buyer's unvouchered counter precisely on the interval boundary and
    // it must present a voucher before it will take another byte.
    // Honest bao bytes, for the reason `serve_then_go_silent` records: the buyer
    // verifies each chunk group as it decodes, so filler would end the pull as
    // corruption long before the voucher round trip this fixture is built around.
    let interval_chunks = usize::try_from(MB_BYTES).unwrap_or(usize::MAX) / CHUNK_SIZE;
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
    write_frame(&mut send, &encode_message(&ClientMessage::VoucherAck)?)
        .await
        .map_err(|e| anyhow::anyhow!("write ack: {e}"))?;

    // Paid, acked — and now quiet. `send` is held open (never finished, never
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
/// upstream already acked (#1145 review).
///
/// Cancellation is not an exotic path here — it is the designed behaviour, and #1134
/// is what made it reachable. The buffered pull now carries `hard_cap: None`, so
/// nothing INSIDE it ends a slow-but-progressing transfer. Everything that does end
/// one is external, and every one of them DROPS the future rather than returning
/// through it: the foreground `outer_pull_deadline`, or the serve future being
/// dropped (client disconnect, node shutdown).
///
/// `VoucherProgress` promises that its copy-back "runs on every return path … so the
/// latest acked totals survive a mid-stream failure". A drop is not a return path.
/// The money is spent the instant the upstream acks, but the watermark lived in the
/// cancelled frame and died with it — so the next pull re-signs a stale nonce, the
/// upstream rejects `StaleNonce`, and `OurVoucherRejected` skips the candidate
/// without a word, for the whole 90-day life of the channel.
///
/// The cancellation here is a short `timeout` that drops the fetch — standing in for
/// the three real droppers — against a stall budget long enough that `PullStalled`
/// cannot be what ends it. The bytes were paid for either way; the only question the
/// test asks is whether we wrote that down.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_cancelled_pull_still_persists_the_acked_watermark() -> Result<()> {
    // Advertise 1.5 MiB but serve only the first 1 MiB, so the buyer is left waiting
    // for a remainder that never comes — with one interval already bought and acked.
    let payload = vec![0x7Du8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

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
        channel_id: B256::repeat_byte(0xD2),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        // A stall budget far longer than the cancellation below, so the inactivity
        // deadline provably is NOT what ends this pull. The drop is.
        Duration::from_mins(2),
        0,
    );

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

    // The upstream acked one 1 MiB voucher before going quiet: nonce 1, 1 MiB of
    // bytes, `ceil(1 MiB × RATE / 1 MiB)` = RATE in amount. That is real USDC, and it
    // must be on the buyer's books even though the pull that spent it never returned.
    anyhow::ensure!(
        progress_log(&recorded)?
            == vec![(
                a_eth.address(),
                U256::from(1),
                U256::from(MB_BYTES),
                U256::from(RATE)
            )],
        "a cancelled pull must persist the watermark the upstream already acked — \
         otherwise the next reuse re-signs a stale nonce and the channel wedges until \
         it expires. Got {:?}",
        progress_log(&recorded)?
    );

    task_a.abort();
    ep_b.close().await;
    ep_a.close().await;
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
    let resp = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    // Two real frames, well under the 1 MiB voucher interval, so no voucher round trip
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
    let resp = signed_response(&req, eth, slash, rate, total_bytes, Some(error))?;
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
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
/// whole bounds rewrite. [`serve_wire_paced`] paces by *voucher interval*, not by
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

/// #1088, BUFFERED receive loop (`client_pull::receive_and_pay`, reached via
/// `Origin::fetch`): an upstream that streams empty `ChunkData` frames forever
/// must be rejected AT ONCE, on the frame itself.
///
/// `ChunkData::validate` has a unit test; this is the one that proves the receive
/// loop CALLS it. With the call reverted to the old ceiling-only check
/// (`if chunk.bytes().len() > CHUNK_SIZE { bail }`), an empty frame passes every
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
        channel_id: B256::repeat_byte(0xE8),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(10),
        EMPTY_CHUNK_STALL_BUDGET,
        0,
    );

    let got = tokio::time::timeout(
        EMPTY_CHUNK_ASSERT_WINDOW,
        Origin::fetch(&origin, hash, u64::MAX),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "the buffered receive loop never returned: it spun on empty ChunkData frames, \
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

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// #1088, WINDOW receive loop (`client_pull::UpstreamPull::next_chunk`, reached
/// via `NodeOrigin::open_progressive_pull`): the same empty-frame stream, driven
/// chunk-by-chunk by the serve path.
///
/// Strictly worse here than on the buffered path, and that is why it gets its own
/// test rather than trusting the shared `validate()` call: this loop's inactivity
/// deadline is re-armed on EVERY read (a per-call budget, not an absolute
/// instant), so an empty frame every few microseconds refreshes it indefinitely.
/// With `validate()` reverted out, `next_chunk` returns `Ok(Some(<empty>))`
/// forever and NOTHING ends the stream.
#[tokio::test(flavor = "multi_thread")]
async fn node_origin_window_empty_chunk_stream_is_rejected_not_spun_on() -> Result<()> {
    let payload = vec![0x5Fu8; 4096];
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
        channel_id: B256::repeat_byte(0xE9),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(10),
        EMPTY_CHUNK_STALL_BUDGET,
        0,
    );

    // The OPEN is honest (a valid signed response), so this must succeed — the
    // hostility is entirely in the frames that follow.
    let (_header, mut pull) = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the progressive open never returned"))?
    .map_err(|miss| {
        anyhow::anyhow!("expected a clean open against the empty-chunk upstream, got {miss:?}")
    })?;

    let drained = tokio::time::timeout(EMPTY_CHUNK_ASSERT_WINDOW, async move {
        loop {
            match pull.next_chunk().await {
                Ok(Some(_)) => {}
                Ok(None) => break Ok(()),
                Err(e) => break Err(e),
            }
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "UpstreamPull::next_chunk spun forever on empty ChunkData frames: each read \
             re-arms the inactivity deadline, so only the non-empty floor ends it (#1088)"
        )
    })?;
    anyhow::ensure!(
        drained.is_err(),
        "an empty ChunkData frame must fail the window pull, not be forwarded as progress"
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// #1134: an upstream that opens honestly and then goes SILENT mid-stream must be
/// abandoned on the INACTIVITY budget and SCORED for it.
///
/// This is the only test that behaviourally separates `PullStalled` from
/// `PullTimeout`, and the separation is the point of the whole deadline split:
///
/// - `PullTimeout` is OUR wall clock expiring. It fires on healthy transfers (a
///   big blob, a slow link), so it must NOT tar the peer — and
///   `node_origin_pull_falls_through_a_stalled_candidate` pins that exoneration.
/// - `PullStalled` is the peer going quiet while we wait, with the deadline reset
///   on every byte of progress. It cannot fire on a healthy transfer, so it is
///   real evidence of an unreachable peer — and must score exactly like one.
///
/// Before the split, this failure shape produced a `PullTimeout` (the whole-blob
/// deadline) and the silent peer was exonerated. Reverting `pull_from_candidate`
/// to `PullDeadlines::whole_transfer(pull_timeout)` restores that: the stalled
/// counter stays 0 and the score stays neutral.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // multi-node fixture setup, like its siblings above
async fn node_origin_mid_stream_silence_scores_stalled_upstream() -> Result<()> {
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
    // stall, not an open-stage one), but far under the 1 MiB voucher interval, so
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
        channel_id: B256::repeat_byte(0xD1),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        // A GENEROUS open budget against a SHORT stall budget — the reverse of the
        // open-stage stall test. If the two were interchangeable the peer would be
        // exonerated by the wrong bound; sizing them this way means only the
        // inactivity deadline can be what ends this pull.
        Duration::from_secs(20),
        Duration::from_secs(2),
        0,
    );

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
    // …and scored: the peer answered, took our request, and then stopped
    // delivering. Unlike every other exonerated arm, THIS one tars the provider.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 1)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 0)?;
    assert_counter(&b_metrics, "node_pull_corruption_total", 0)?;

    anyhow::ensure!(
        local_rep.score(a_id) < 0.5,
        "a mid-stream stall must drop the local score below neutral, got {}",
        local_rep.score(a_id)
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// The mirror image of the test above, and the line between them is the whole point: a peer
/// that never sent a FIRST byte must NOT be scored `Unreachable` (#1145 review).
///
/// `PullStalled` earns the right to gossip about a peer from the deadline's reset — a clock
/// that resets on every byte can only fire on a peer that stopped delivering. That argument
/// needs a byte to have arrived. Before the first one there has been no reset, and the clock
/// is measuring something else entirely: the server's TIME TO FIRST BYTE, which scales with
/// blob size, because the serve path writes the `StreamResponse` and only then materialises
/// the whole bao wire encoding (`export_bao_range`) before it can emit chunk #1.
///
/// So a 1 GiB blob — the default `max_blob_size_mb` — read off a cold disk, or served by a
/// node already streaming to several peers, could blow the 20 s default stall budget doing
/// exactly what it was asked. The requester then scored it `Unreachable`: a local EWMA hit
/// against an honest server, for the crime of being big.
///
/// The fix gives that wait the same verdict the OPEN stage already gives an identical wait —
/// `PullTimeout`, exonerating — on the same grounds: a bound of ours elapsing over bounded
/// server work says nothing about the peer. Nothing is given up that the open stage has not
/// already given up, and the pull still fails and still yields the candidate slot.
///
/// Zero prefix chunks is the entire fixture. Its sibling above sends three, and must still
/// score — the two together pin the boundary at exactly one byte.
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
        channel_id: B256::repeat_byte(0x7E),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        // A generous OPEN budget, so the open stage is provably not what ends this: the peer
        // does answer, promptly and correctly. Only the first-chunk wait can be what fires.
        Duration::from_secs(20),
        Duration::from_secs(2),
        0,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
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
        channel_id: B256::repeat_byte(0x8B),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        // Both budgets generous: the error must be what ends this pull, not a deadline.
        // Sized so a regression cannot pass by accidentally timing out into an exonerating
        // `PullTimeout` arm instead.
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

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

    // Nothing gossiped, and the local score untouched: the peer answered honestly.
    // The mirror of the stall test's `< 0.5`: a stall drops the score below neutral, an
    // honest refusal must not touch it.
    anyhow::ensure!(
        local_rep.score(a_id) >= 0.5,
        "an honest mid-stream refusal must leave the local score neutral, got {}",
        local_rep.score(a_id)
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// #1145 review (4th `{e:?}` site) — a non-`VoucherRejected` `StreamError` arriving in reply
/// to the CLOSING VOUCHER lands in the receive loop's voucher-slot handler
/// (`resolve_voucher_slot`, once the optimistic loop of #1484). Round 2 typed the three
/// mid-stream receive sites but left this one stringifying the wire code, so an honest
/// `Overloaded`/`NotFound` fell through every downcast to the `Unreachable` catch-all —
/// scoring a reachable, honestly-answering peer as a dead node.
///
/// Driven through the REAL path (the server delivers the whole payload, reads the closing
/// voucher, then replies `Overloaded`), because — as the mid-stream sibling spells out — an
/// assertion against `classify_pull_failure`'s ladder would pass with the `bail!("{e:?}")`
/// restored: the classifier was never the thing that broke. `node_pull_refused_total` is
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
        channel_id: B256::repeat_byte(0x4D),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        // Generous budgets: the refusal must end this pull, not a deadline.
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// #1145 review — a WEDGED provider must be skipped for ALL hashes until its channel expires,
/// not just the one that wedged it. The `(peer, hash)` negative-cache entry the wedge writes
/// covers only the same blob for 30s; the provider-wide `wedged_providers` entry (held to the
/// channel's expiry) is what a miss for a DIFFERENT blob needs. Without it, `try_reuse_live`
/// (expiry-gated) hands the dead channel back and re-wedges it on the next miss.
///
/// Fail-on-revert: drop the `provider_is_wedged` filter in `probe_and_rank` and the second
/// pull re-selects A, re-wedging it — `node_pull_channel_wedged_total` becomes 2, not 1.
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
    // A rejects every closing voucher with `StaleNonce` → wedges the channel on any pull it
    // is selected for. The server loops, so it handles both pulls.
    let task_a = spawn_a_voucher_rejecting_server(
        ep_a.clone(),
        Arc::clone(&a_eth),
        slash_domain(),
        payload1.clone(),
        u64::try_from(payload1.len()).unwrap_or(u64::MAX),
        RATE,
        VoucherRejectReason::StaleNonce,
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
        channel_id: B256::repeat_byte(0xA1),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let mut addr_map = HashMap::new();
    addr_map.insert(a_dht, a_eth.address());
    let origin = build_origin_multi_hash(
        &ep_b,
        b_dht,
        &[hash1, hash2],
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        &[a_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
    );

    // Pull 1 (hash1): A wedges its channel. One wedge event.
    let got1 = tokio::time::timeout(
        Duration::from_secs(30),
        Origin::fetch(&origin, hash1, u64::MAX),
    )
    .await
    .map_err(|_| anyhow::anyhow!("pull 1 never ended"))?
    .map_err(|e| anyhow::anyhow!("pull 1: {e}"))?;
    anyhow::ensure!(matches!(got1, OriginFetch::NotFound), "pull 1 must refuse");
    assert_counter(&b_metrics, "node_pull_channel_wedged_total", 1)?;

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
    assert_counter(&b_metrics, "node_pull_channel_wedged_total", 1)?;

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
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
/// The consequence is the one the drained-channel test exists to prevent, reached by another
/// road: the channel stays in the store, `try_reuse_live` (which gates on expiry alone) hands
/// it straight back on the next miss, and this node re-presents a voucher it cannot honour on
/// every pull until it expires — logging a `debug!` invisible at the default `RUST_LOG=info`.
///
/// Driven through the REAL receive loop, for the reason the sibling test above spells out: an
/// assertion against `classify_pull_failure`'s ladder alone would pass with the bug restored,
/// because the classifier was never the thing that was broken. The counter that proves the
/// remedy ran is `node_pull_channel_wedged_total` — reachable only if the code survived the
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
            reason: VoucherRejectReason::InsufficientDeposit,
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
        channel_id: B256::repeat_byte(0x9C),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::clone(&retired),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        // Generous, as above: the rejection must be what ends this pull, not a deadline.
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

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
    assert_counter(&b_metrics, "node_pull_channel_wedged_total", 1)?;
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

    // Our payment fault, not the peer's: it is not scored, here or over gossip.
    assert_counter(&b_metrics, "node_pull_unreachable_total", 0)?;
    anyhow::ensure!(
        (local_rep.score(a_id) - 0.5).abs() < f64::EPSILON,
        "the provider's score must stay neutral, got {}",
        local_rep.score(a_id)
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
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
    // Small budgets: the deadline arithmetic is asserted in the unit test named above, so
    // what these buy is a fast, non-flaky exercise of the real failover path.
    let per_candidate = Duration::from_secs(2);
    let stall_budget = Duration::from_secs(2);

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
    let channel_id = B256::repeat_byte(0xC7);
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;

    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![s1_dht, s2_dht, a_dht],
        addr_map,
        per_candidate,
        stall_budget,
        0,
    );

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

    let bytes = fetched
        .collect_to_bytes()
        .await
        .map_err(|e| anyhow::anyhow!("collect: {e}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "the loop gave up before reaching the healthy candidate #3 — two silent \
                 candidates consumed a budget the outer deadline had not allowed for"
            )
        })?;
    anyhow::ensure!(bytes == payload, "wrong bytes from candidate #3");

    // Both silent candidates were classified as stalls (not as our own deadline firing),
    // which is what proves they were abandoned on the STALL bound — the stage whose budget
    // this test exists to protect — rather than on some other clock.
    assert_counter(&b_metrics, "node_pull_stalled_total", 2)?;
    assert_counter(&b_metrics, "node_pull_timeout_total", 0)?;

    ep_b.close().await;
    ep_a.close().await;
    ep_s1.close().await;
    ep_s2.close().await;
    task_a.abort();
    task_s1.abort();
    task_s2.abort();
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
/// `serve_wire_paced` paces by voucher interval and never sleeps: nothing in the
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
    let payload = vec![0x51u8; 6 * CHUNK_SIZE];
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
        channel_id: B256::repeat_byte(0x51),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        SLOW_PULL_OPEN_BUDGET,
        // Comfortably above the 600 ms inter-frame gap: this upstream is slow, not
        // silent, so the inactivity bound must never fire.
        Duration::from_secs(5),
        0,
    );

    let started = std::time::Instant::now();
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("a slow-but-healthy pull must complete, got: {e}"))?;
    let bytes = fetched.collect_to_bytes().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "a slow-but-healthy pull returned NotFound: the transfer was killed by a \
             whole-blob deadline it should no longer have (#1134)"
        )
    })?;
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

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
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
/// being dead — and gossiped that verdict network-wide. (`NotFound` is
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
    let channel_id = B256::repeat_byte(0x4E);
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
    let store_n = Arc::new(MemoryChannelStateStore::new());
    // A funded, known channel — so the refusal is unambiguously "no blob" and not
    // an unknown-channel rejection wearing the same collapsed wire code.
    store_n.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;
    let metrics_n = Arc::new(Metrics::new());
    let handler_n = build_handler_full(
        n_id,
        &n_eth,
        &metrics_n,
        permissive_limiter(&metrics_n),
        cache_n,
        store_n as Arc<dyn ChannelStateStore>,
        STALL_RATE,
        &domains,
        0,
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
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        permissive_limiter(&metrics_a),
        cache_a,
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![n_dht, a_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // N refuses; the loop falls through to A, which delivers.
    let fetched = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("node-origin fetch failed: {e}"))?;
    let bytes = fetched
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected the blob from the candidate that holds it"))?;
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
        refetched
            .collect_to_bytes()
            .await?
            .is_some_and(|b| b.as_ref() == payload.as_slice()),
        "the second fetch must still deliver the blob from A"
    );
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    assert_counter(&b_metrics, "node_pull_success_total", 2)?;

    ep_b.close().await;
    ep_n.close().await;
    ep_a.close().await;
    task_n.abort();
    task_a.abort();
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
        channel_id: B256::repeat_byte(0x6B),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_negative_cache(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
        NegativeProbeCache::with_capacity_and_ttl(16, TINY_CACHE_TTL),
    );

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

    task_a.abort();
    ep_b.close().await;
    ep_a.close().await;
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
        channel_id: B256::repeat_byte(0x4E),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(got.is_none(), "a refused pull must not surface bytes");
    assert_counter(&b_metrics, "node_pull_refused_total", 1)?;
    let count = counter_value(&b_metrics, "probe_post_eviction_failures_total")?;

    task_a.abort();
    ep_b.close().await;
    ep_a.close().await;
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
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0x1E),
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// #840 over the real orchestration: an honest upstream holds and would serve a
/// blob larger than B's `max_blob_size` ceiling. The buyer must reject the
/// oversized `total_bytes` claim before buffering — the fetch is a clean
/// `NotFound`, the `node_pull_too_large` counter moves, and (crucially) the
/// provider is NOT scored: a buyer-side ceiling is OUR policy, not the provider's
/// fault, so no observation is emitted and its local score stays neutral.
///
/// This exercises `pull_from_candidate` passing `deps.config.max_blob_size_bytes`
/// (the loopback test calls `stream_fetch_tracked` directly and bypasses it).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_oversized_claim_is_rejected_without_scoring() -> Result<()> {
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);
    // Buyer ceiling well below the 1.5 MiB blob → the gate fires.
    let ceiling: u64 = 1_048_576;
    anyhow::ensure!(total_bytes > ceiling, "fixture must exceed the ceiling");

    // --- Node A: honest, unlimited server holding the blob. -------------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0, // server ceiling unlimited — it would happily serve the full blob.
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
    let origin = provisioned_origin_with_ceiling(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        ceiling,
    );

    let got = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch: {e}"))?;
    anyhow::ensure!(
        matches!(got, OriginFetch::NotFound),
        "an over-ceiling claim must not surface bytes (NotFound)"
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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    let channel_id = B256::repeat_byte(0xA2);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0, // server blob ceiling unlimited — isolate the RATE gate.
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
    let origin = provisioned_origin_with_ceiling(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
        0,
    );

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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok(())
}

/// The #852 regression: a second cache-miss pull to the same provider **reuses**
/// the buyer channel and resumes from the persisted voucher watermark, so it
/// signs `nonce = 3, 4 …` (not a stale `nonce = 1`) and the upstream accepts it.
///
/// Before the fix, the first pull's progress was never persisted, so the second
/// pull re-signed from zero and the upstream rejected it (`StaleNonce`) — the
/// second fetch would be a `NotFound`. Here both fetches deliver the blob and the
/// persisted log advances monotonically (nonce 2 → 4, bytes 1.5 MiB → 3 MiB).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn node_origin_reused_channel_resumes_voucher_progress() -> Result<()> {
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
    let channel_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

    // First pull: opens the channel, pays nonce 1..2, persists the watermark.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch failed: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("first fetch returned NotFound"))?;
    anyhow::ensure!(
        first.as_ref() == payload.as_slice(),
        "first pull bytes mismatch"
    );

    // Second pull: REUSES the channel, resumes from the persisted watermark, and
    // the upstream accepts the continued nonces — this is the bug's fix.
    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch failed: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "second fetch returned NotFound — stale voucher rejected (the #852 bug)"
            )
        })?;
    anyhow::ensure!(
        second.as_ref() == payload.as_slice(),
        "second pull bytes mismatch"
    );

    // The persisted watermark advanced monotonically across the two pulls rather
    // than resetting: nonce 2 → 4, with cumulative bytes and amount doubling.
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
            (
                a_eth.address(),
                U256::from(2),
                U256::from(expected_wire),
                expected_amount
            ),
            (
                a_eth.address(),
                U256::from(4),
                U256::from(expected_wire).saturating_mul(U256::from(2)),
                expected_amount.saturating_mul(U256::from(2)),
            ),
        ],
        "expected two monotonically-advancing progress entries, got {log:?}"
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    let channel_id = B256::repeat_byte(0xA1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
    );

    // The pull delivers the verified bytes even though persisting the watermark
    // failed — the persist error must not discard already-paid-for content.
    let bytes = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("fetch failed: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("persist failure must not turn the pull into NotFound"))?;
    anyhow::ensure!(
        bytes.as_ref() == payload.as_slice(),
        "delivered bytes mismatch"
    );

    // …but the failure is observable: the delivery still scored a clean success,
    // and the persist-failure counter moved exactly once.
    assert_counter(&b_metrics, "node_pull_success_total", 1)?;
    assert_counter(&b_metrics, "node_pull_progress_persist_failures_total", 1)?;

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    received: u64,
    acks: u64,
    completed: bool,
    hash_ok: bool,
}

async fn read_client(recv: &mut iroh::endpoint::RecvStream) -> Result<ClientMessage> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("leaf read frame: {e}"))?;
    let (msg, _) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("leaf decode: {e}"))?;
    Ok(msg)
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
/// closes the connection immediately after the n-th `VoucherAck` — the #856
/// abandon shape. The wire carries the bao verified-stream (content + proof,
/// ADR 038), so it paces on the bao-encoded WIRE size and decodes the buffer
/// back to plaintext to verify the content hash.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn leaf_paced_pull(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    channel_id: B256,
    hash: Hash,
    rate: u64,
    drop_after_acks: Option<u64>,
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
        voucher_interval_mb: None,
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: 0x9001,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write req: {e}"))?;

    let resp = match read_client(&mut recv).await? {
        ClientMessage::StreamResponse(r) => r,
        other => anyhow::bail!("expected StreamResponse, got {other:?}"),
    };
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp.error);
    let total = resp.body.total_bytes;
    // B forwards + meters WIRE bytes (bao: content + interleaved proof), so the
    // closing-voucher / completeness boundary is the bao-encoded size, not the
    // content `total_bytes`. The per-interval boundary is unchanged — both sides
    // count the same forwarded wire bytes into `interval_bytes`.
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(total, &bao_tree::ChunkRanges::all());
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    let mut acks: u64 = 0;
    loop {
        match read_client(&mut recv).await? {
            ClientMessage::ChunkData(chunk) => {
                buf.extend_from_slice(chunk.bytes());
                let len = chunk.bytes().len() as u64;
                cumulative = cumulative.saturating_add(len);
                unvouchered = unvouchered.saturating_add(len);
                let boundary = unvouchered >= interval_bytes && interval_bytes > 0;
                let closing = cumulative >= expected_wire && unvouchered > 0;
                if boundary || closing {
                    acks += 1;
                    let amount = U256::from(cumulative)
                        .saturating_mul(U256::from(rate))
                        .div_ceil(U256::from(MB_BYTES));
                    let signed = Voucher {
                        channel_id,
                        amount,
                        nonce: U256::from(acks),
                        bytes_delivered: U256::from(cumulative),
                        token: TOKEN,
                    }
                    .sign(leaf_eth.as_ref(), &voucher_dom())
                    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
                    write_client(
                        &mut send,
                        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
                    )
                    .await?;
                    match read_client(&mut recv).await? {
                        ClientMessage::VoucherAck => {}
                        other => anyhow::bail!("expected VoucherAck, got {other:?}"),
                    }
                    unvouchered = 0;
                    if drop_after_acks == Some(acks) {
                        conn.close(0u32.into(), b"leaf-drop");
                        return Ok(LeafOutcome {
                            received: cumulative,
                            acks,
                            completed: false,
                            hash_ok: false,
                        });
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
    leech_caps: Option<LeechCaps>,
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Arc<Metrics>,
    Arc<LocalReputation>,
    Option<Arc<LeechGovernor>>,
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
        leech_caps,
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
/// Each leaf is `(channel_id, funder, voucher_signer, deposit)`. The two address
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
    // Seed-leech caps to enable the governor on node B; the governor is built
    // over B's own `Metrics` (so leech counters land where tests assert them) and
    // returned so a test can pre-exhaust it before serving (#1254).
    leech_caps: Option<LeechCaps>,
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
    Option<Arc<LeechGovernor>>,
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
    let (origin, recorded) = provisioned_origin_with_deadlines(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        ab_channel_id,
        b_buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        node_pull_deadlines,
    );

    // B's empty cache (the tee fills it) and the leaf's channel in B's store.
    let cache_tmp = tempfile::tempdir()?;
    let cache_b =
        decdn_cache::CacheEngine::open(cache_tmp.path(), vec![], engine_max_blob_mb).await?;
    let cache_handle = cache_b.clone();
    // Leak the tempdir guard for the test's lifetime (kept alive by the returned
    // engine's open store anyway).
    std::mem::forget(cache_tmp);
    let store_b = Arc::new(MemoryChannelStateStore::new());
    for (leaf_channel_id, leaf_funder, leaf_voucher_signer, leaf_deposit) in leaves {
        store_b.record(&ChannelState::new(
            *leaf_channel_id,
            *leaf_funder,
            *leaf_voucher_signer,
            TOKEN,
            *leaf_deposit,
        ))?;
    }
    let limiter = permissive_limiter(&b_metrics);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    // Build the governor over B's own `Metrics` so leech counters land where the
    // tests assert them, and return it so a test can pre-exhaust it before serving.
    let leech_governor =
        leech_caps.map(|caps| Arc::new(LeechGovernor::new(caps, Arc::clone(&b_metrics))));
    let returned_governor = leech_governor.clone();
    let handler_b = build_handler_full_configured(
        b_id,
        &b_eth,
        &b_metrics,
        limiter,
        cache_b,
        store_b as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        max_blob_size_bytes,
        16,
        |deps| {
            // Window-paced pull-through: the deadline accommodates the full
            // discover→probe→pull, and the window is the default ~1 MiB (one interval).
            deps.pull_through = Some(Duration::from_secs(20));
            deps.set_window_pull_through(
                Arc::new(origin),
                decdn_cache::Bytes::new(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES),
            );
            deps.leech_governor = leech_governor;
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
        returned_governor,
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
    let hash = Hash::new(payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let (cache_a, hash_a, tmp_a) = cache_with_blob(payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    std::mem::forget(tmp_a);
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        ab_channel_id,
        b_buyer_addr,
        b_buyer_addr,
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    Ok((a_id, addr_a, a_eth, ep_a, task_a))
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_serves_and_caches_full_blob() -> Result<()> {
    let payload = vec![0xCDu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xA1);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x1F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
            None,
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
    // The headline #856 behavior: PAYLOAD_LEN (1.5 MiB) exceeds the default
    // ~1 MiB window, so the pull MUST have paused at the window frontier and
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
    // B's buyer channel to A advanced under the DECOUPLED window-paced cadence
    // (#1621 B2 part 2), which differs from the fused path's single-open shape:
    //
    //   * nonce 3 (not 2). The pull leg opens the blob in MORE THAN ONE span: the
    //     ~1 MiB window pause at the frontier forces a SECOND upstream open (the
    //     `node_pull_through_window_paused_total >= 1` assertion above proves the
    //     pause fired), so the buyer signs one extra voucher — one per open.
    //   * 1_579_008 WIRE bytes (not the single-span `bao_encoded_size(all)` of
    //     1_578_944). Under ADR 038 the node-to-node payment meters WIRE (the bao
    //     stream: content + interleaved proof). Re-opening at the span boundary
    //     re-emits that boundary's bao parent once, adding exactly 64 redundant
    //     proof bytes: 1_578_944 + 64 = 1_579_008.
    //   * amount 17. Voucher amounts round up PER interval delta, not once over the
    //     cumulative total (protocol-mandated), so the sum of per-interval ceilings
    //     is 17 — one more than a single cumulative ceiling. B pays A for exactly
    //     the wire A delivered.
    anyhow::ensure!(
        progress_log(&recorded)?
            == vec![(
                a_eth.address(),
                U256::from(3),
                U256::from(1_579_008),
                U256::from(17)
            )],
        "expected B's upstream watermark at the full blob, got {:?}",
        progress_log(&recorded)?
    );

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
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
    // Many 1-MiB voucher intervals, so plenty of re-check boundaries remain
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
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, _leech_gov) =
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
            None,
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
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
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, _leech_gov) =
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
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
    let (handler_b, b_target, ep_b, _recorded, _cache_b, b_metrics, _local_rep, _leech_gov) =
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

/// #1054: an empty (0-byte) blob served via the fused window pull-through path.
/// This exercises the progressive/tee triangle — `open_progressive_pull` →
/// `UpstreamPull::finish` → tee promote → downstream `leaf_paced_pull` verify —
/// which never calls `decode_verified_range`, so the buffered-path e2e
/// (`client_delivers_empty_blob`) does not cover it. For 0 wire bytes the window
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
    let (handler_b, b_target, ep_b, recorded, cache_b, _b_metrics, _local_rep, _leech_gov) =
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
            None,
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
        u64::try_from(entry.2).unwrap_or(u64::MAX) == total_bytes,
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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

/// A resumed cache-miss request (`byte_offset > 0`) must NOT engage the fused
/// window path: the incremental whole-blob BLAKE3 is only valid from offset 0,
/// so the handler gates the fused serve on `req.byte_offset == 0` and falls a
/// resumed miss back to the buffered path (`client.rs` §window-paced gate). With
/// the window provider attached but no buffered origin on B's empty cache, the
/// buffered fallback cleanly refuses. The regression this guards: dropping the
/// offset-0 gate would route a resumed request into the fused path, which pulls
/// and verifies from byte 0 and would mis-serve / mis-cache the blob (#856).
#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_resumed_offset_falls_back_not_fused() -> Result<()> {
    use alloy::signers::SignerSync;

    let payload = vec![0x7Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x7F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
            None,
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;

    // Send a request resuming from one interval in (byte_offset > 0), with a
    // valid ownership binding so authorization is NOT the reason for refusal —
    // the offset gate must be.
    let conn = leaf_ep
        .connect(b_target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("leaf connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("leaf open_bi: {e}"))?;
    let binding_hash = binding_signing_hash(leaf_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = leaf_eth.sign_hash_sync(&binding_hash)?.as_bytes().to_vec();
    let ext = StreamRequestExt {
        voucher_interval_mb: None,
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: leaf_channel_id.into(),
        byte_offset: MB_BYTES,
        byte_len: 0,
        timestamp_us: 0x9007,
    };
    let payload_bytes =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame(&mut send, &payload_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("write req: {e}"))?;

    let resp = match read_client(&mut recv).await? {
        ClientMessage::StreamResponse(r) => r,
        other => anyhow::bail!("expected StreamResponse, got {other:?}"),
    };
    // The resumed miss is refused by the buffered fallback (no buffered origin),
    // NOT served by the fused path.
    anyhow::ensure!(
        !resp.body.ok,
        "resumed offset>0 miss must be refused, not served; error={:?}",
        resp.error
    );
    conn.close(0u32.into(), b"done");

    // The serve-miss pull-through must never have run: no window pause, no
    // upstream verify — and crucially B must have made NO upstream pull (empty
    // progress log) and cached NOTHING. A regression that dropped the offset-0
    // gate would trip at least the upstream pull (non-empty log).
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_window_paused_total")? == 0,
        "serve-miss window pause must not fire for a resumed (offset>0) request"
    );
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_upstream_verify_failed_total")? == 0,
        "serve-miss upstream verify must not run for a resumed (offset>0) request"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "B must not have pulled upstream for a resumed (offset>0) miss, got {:?}",
        progress_log(&recorded)?
    );
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "B must not have cached anything for a refused resumed request"
    );

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
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
    let (handler_b, b_target, ep_b, recorded, _cache_b, _b_metrics, _local_rep, _leech_gov) =
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
            None,
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

    // The headline #856 bound under the DECOUPLED pull leg (#1621 B2 part 2). The
    // fused loop bounded the TOTAL bytes pulled to ~one window; the decoupled leg
    // paces on the CONTENT frontier (`WindowPacer`, ADR 037) and keeps at most one
    // window of UNPAID content in flight, so once the leaf's single paid interval
    // sits within a window of the blob's end the leg finishes the whole sub-2-window
    // blob. The maintainer decision below allows that, so the real invariant is on
    // the UNRECOUPED lead, not the total pulled: `upstream_bytes - paid <= window +
    // group`.
    let log = progress_log(&recorded)?;
    let upstream_bytes: u64 = log.last().map_or(0, |(_, _, bytes, _)| {
        u64::try_from(*bytes).unwrap_or(u64::MAX)
    });
    // What the leaf actually paid for: it acked exactly one 1-MiB voucher interval
    // before dropping (asserted above), so the content it received is the concrete
    // stand-in for its paid frontier (~one window).
    let paid = outcome.received;
    let one_window = decdn_common::config::DEFAULT_PULL_AHEAD_BYTES;
    // `upstream_bytes` is the bao WIRE (content + interleaved proof, ADR 038); one
    // chunk group of slack absorbs the boundary chunk group plus the proof overhead
    // over the window. This is the #1644-review bound the anvil proof (Task 14) uses.
    let group = decdn_cache::CHUNK_GROUP_BYTES;
    anyhow::ensure!(
        upstream_bytes.saturating_sub(paid) <= one_window + group,
        "B's UNRECOUPED upstream lead ({upstream_bytes} pulled - {paid} paid) must be bounded to \
         ~one window ({one_window}), not run open-ended against the {total_bytes}-byte blob"
    );
    // Caching a fully-pulled sub-2-window blob is now ALLOWED: the #856 spend bound
    // holds as bounded UNRECOUPED lead (above), not as total-pulled, so a leaf that
    // paid one interval of a 1.5-window blob may still leave B holding the finished
    // fill. No promotion assertion either way — this test polices spend, not caching.

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_insufficient_deposit_refuses_before_pulling() -> Result<()> {
    // #856 pre-flight deposit guard: with a finite `max_blob_size_bytes`, a
    // channel whose remaining deposit cannot cover the worst-case blob cost is
    // refused (signed `NotFound`) BEFORE any upstream pull — no USDC fronted.
    let payload = vec![0x9Eu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA3);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x3F);
    // Deposit 100 µUSDC clears `dispatch.rs`'s pre-spend miss floor (#1519 — one
    // credit window at `RATE`, i.e. 10) but not this handler's 64 MiB blob-size
    // ceiling (`min_payment(64 MiB, RATE)` = 640), so the guard under test is
    // `window.rs`'s — not the hoisted floor, and not the size gate (the blob is
    // well under 64 MiB). A deposit below 10 would be refused by the floor first
    // and this test would silently stop covering `window.rs` at all.
    let max_blob_size_bytes = 64 * 1024 * 1024;
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
        build_node_b(
            a_id,
            a_addr,
            a_eth.address(),
            hash,
            ab_channel_id,
            &b_buyer,
            leaf_channel_id,
            leaf_eth.address(),
            U256::from(100u64),
            max_blob_size_bytes,
            None,
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_leech_stall_refuses_without_spinning() -> Result<()> {
    // #856 (C1 regression): when a seed-leech cap denies the speculative pull
    // mid-stream while the request has no completed interval to recoup, the serve
    // loop must REFUSE (abandon the partial fill) rather than busy-spin with no
    // await. We pin a per-peer opening allowance of one chunk and a 0% share ratio
    // (no growth) with the global budget off, so after the very first chunk the
    // peer is over its allowance with nothing collectable — the exact stall shape.
    // The whole interaction must resolve well within the deadline (a regression
    // that reintroduces the spin hangs here), B must not cache the partial blob,
    // and the share-ratio pause must have fired exactly once.
    let payload = vec![0x5Au8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA4);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x4F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
            Some(LeechCaps::new_unchecked(LeechCapsConfig {
                max_unrecouped_leech_bytes: Bytes::new(0),
                initial_allowance_bytes: Bytes::new(CHUNK_SIZE as u64),
                share_ratio_percent: Percent::new(0),
            })),
        )
        .await?;
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let resolved = tokio::time::timeout(
        Duration::from_secs(10),
        leaf_paced_pull(
            &leaf_ep,
            b_target,
            leaf_node_id,
            &leaf_eth,
            leaf_channel_id,
            hash,
            RATE,
            None,
        ),
    )
    .await;
    anyhow::ensure!(
        resolved.is_ok(),
        "window serve busy-spun under a seed-leech stall instead of refusing (C1)"
    );

    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "a leech-stalled serve must not promote the partial blob"
    );
    let upstream_bytes: u64 = progress_log(&recorded)?
        .last()
        .map_or(0, |(_, _, bytes, _)| {
            u64::try_from(*bytes).unwrap_or(u64::MAX)
        });
    anyhow::ensure!(
        upstream_bytes < PAYLOAD_LEN as u64,
        "a leech-stalled serve must not pull the whole blob ({upstream_bytes} bytes)"
    );
    // The share-ratio cap denied the speculative pull (the loop re-checks before
    // each pull, so it fires once per stalled iteration before the refusal).
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_share_ratio_paused_total")? >= 1,
        "the share-ratio pause must have fired at least once"
    );

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
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

/// Read one frame, require it to be a `Voucher`, and ack it — the per-interval
/// exchange [`serve_wire_paced`] performs at each 1 MiB boundary.
async fn read_voucher_write_ack(
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<()> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
    let (msg, _) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    let ClientMessage::Voucher(_) = msg else {
        anyhow::bail!("paced upstream: expected a Voucher");
    };
    write_frame(send, &encode_message(&ClientMessage::VoucherAck)?)
        .await
        .map_err(|e| anyhow::anyhow!("write ack: {e}"))?;
    Ok(())
}

/// Like [`serve_wrong_bytes`], but serves `wire` (a bao verified-stream,
/// possibly corrupted mid-way) with the REAL per-interval voucher pacing:
/// `total_bytes` (the CONTENT size) is advertised separately from the wire
/// length, and a voucher is read + acked at every 1 MiB interval boundary of
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
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;
    let interval_bytes = MB_BYTES;
    let mut unvouchered: u64 = 0;
    for chunk in wire.chunks(CHUNK_SIZE) {
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
            read_voucher_write_ack(&mut send, &mut recv).await?;
            unvouchered = 0;
        }
    }
    if unvouchered > 0 {
        read_voucher_write_ack(&mut send, &mut recv).await?;
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
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, local_rep, _leech_gov) =
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
            None,
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_mid_stream_corruption_scores_upstream_not_local() -> Result<()> {
    // #915 review: a corrupt group MID-stream — past the first voucher interval,
    // with plenty of wire still to come — kills the verifying decoder while the
    // node is still forwarding, so the failure surfaces mid-stream rather than at
    // the end. This is the dominant real-world corruption shape. It must be
    // classified as an UPSTREAM fault, not a local store fault: abandon the pull
    // early (bounded spend), not promote, fire `upstream_verify_failed`, and score
    // A `Corruption`.
    //
    // Construction: the HONEST whole-blob bao wire for a 1.5 MiB payload
    // (multi-interval, so one voucher exchange completes before the corruption),
    // with a single byte flipped ~1.1 MiB in — every group before it verifies,
    // the containing group fails.
    let payload = vec![0xB7u8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);
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
    // byte past the first 1 MiB voucher interval.
    let mut wire = combined
        .get(8..)
        .ok_or_else(|| anyhow::anyhow!("combined encoding shorter than its header"))?
        .to_vec();
    let corrupt_at = 1_150_000usize;
    let byte = wire
        .get_mut(corrupt_at)
        .ok_or_else(|| anyhow::anyhow!("corruption offset outside the wire"))?;
    *byte ^= 0xFF;

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) = spawn_paced_lying_node_a(wire, total_bytes).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x7A);
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, local_rep, _leech_gov) =
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
            None,
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

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

/// A leaf that takes the first interval of bytes, then signs and sends a voucher
/// that *underpays* (a token amount well below the rate floor) instead of paying.
/// Drives B's `collect_voucher` to `VoucherOutcome::Rejected` mid-window. Returns
/// once it observes B's `StreamError` rejection (or the stream drops).
async fn leaf_underpays_first_voucher(
    leaf_ep: &iroh::Endpoint,
    target: EndpointAddr,
    leaf_node_id: B256,
    leaf_eth: &Arc<PrivateKeySigner>,
    channel_id: B256,
    hash: Hash,
) -> Result<()> {
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
        voucher_interval_mb: None,
        binding: Some(ClientBinding {
            ethereum_address: leaf_eth.address().into(),
            binding_signature,
        }),
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id.into(),
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

    let resp = match read_client(&mut recv).await? {
        ClientMessage::StreamResponse(r) => r,
        other => anyhow::bail!("expected StreamResponse, got {other:?}"),
    };
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp.error);
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);

    // Read chunks until the first interval boundary, then underpay it.
    let mut cumulative: u64 = 0;
    loop {
        match read_client(&mut recv).await? {
            ClientMessage::ChunkData(chunk) => {
                cumulative = cumulative.saturating_add(chunk.bytes().len() as u64);
                if cumulative >= interval_bytes {
                    break;
                }
            }
            ClientMessage::StreamEnd => anyhow::bail!("stream ended before the first interval"),
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }

    let signed = Voucher {
        channel_id,
        amount: U256::from(1u64), // far below the rate floor for one interval
        nonce: U256::from(1u64),
        bytes_delivered: U256::from(cumulative),
        token: TOKEN,
    }
    .sign(leaf_eth.as_ref(), &voucher_dom())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client(
        &mut send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
    )
    .await?;

    // B must reject the underpayment. A StreamError (the explicit rejection) or a
    // dropped stream (EOF) both signal the refusal.
    match read_client(&mut recv).await {
        Ok(ClientMessage::StreamError(_)) | Err(_) => Ok(()),
        Ok(other) => anyhow::bail!("expected a StreamError for the underpayment, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_underpaid_voucher_abandons_bounded() -> Result<()> {
    // #856: a leaf that underpays a mid-window voucher must be cleanly rejected
    // (VoucherOutcome::Rejected), B must abandon the partial fill (nothing cached),
    // its upstream spend stays bounded to ~one window, and the client-abandoned
    // counter fires.
    let payload = vec![0x7Cu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA7);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x7F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
            None,
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
        leaf_channel_id,
        hash,
    )
    .await?;

    // Let B observe the rejection and abandon.
    tokio::time::sleep(Duration::from_millis(300)).await;

    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "an underpaid serve must not promote the partial blob"
    );
    let upstream_bytes: u64 = progress_log(&recorded)?
        .last()
        .map_or(0, |(_, _, bytes, _)| {
            u64::try_from(*bytes).unwrap_or(u64::MAX)
        });
    let one_window = decdn_common::config::DEFAULT_PULL_AHEAD_BYTES;
    // The window bounds CONTENT bytes, but the upstream watermark meters WIRE bytes
    // (bao content + interleaved proof, ADR 038), so one window of content costs one
    // window + its proof overhead (~4 KiB on a 1 MiB window) plus up to a group of
    // boundary overshoot. One chunk group of slack covers both.
    anyhow::ensure!(
        upstream_bytes <= one_window + decdn_cache::CHUNK_GROUP_BYTES,
        "B's upstream spend ({upstream_bytes}) must stay bounded to ~one window ({one_window})"
    );
    assert_counter(&b_metrics, "node_pull_through_client_abandoned_total", 1)?;

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_global_budget_exhausted_refuses_admission() -> Result<()> {
    // #856: when the node-wide unrecouped-leech budget is already exhausted (by
    // unrelated speculative traffic), a fresh speculative pull is refused at
    // admission — signed `NotFound`, no upstream spend, nothing cached — and the
    // budget pause metric fires.
    let payload = vec![0x6Bu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA5);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x5F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, leech_gov) =
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
            // `new_unchecked`: a tiny global budget below the opening window, so the
            // global circuit breaker binds on the first admission (the scenario under
            // test). `LeechCaps::new` rejects this pairing by design.
            Some(LeechCaps::new_unchecked(LeechCapsConfig {
                max_unrecouped_leech_bytes: Bytes::new(CHUNK_SIZE as u64),
                initial_allowance_bytes: Bytes::new(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES),
                share_ratio_percent: Percent::new(100),
            })),
        )
        .await?;
    // Pre-exhaust the global budget through an unrelated peer, on the same governor
    // the handler now holds.
    let Some(gov) = leech_gov else {
        anyhow::bail!("leech governor built from caps");
    };
    gov.record_pulled(
        &[0xEEu8; 32],
        decdn_common::config::DEFAULT_PULL_AHEAD_BYTES,
    );
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let leaf_sk = fresh_key();
    let leaf_node_id = B256::from(*leaf_sk.public().as_bytes());
    let (leaf_ep, _) = local_endpoint(leaf_sk, vec![]).await?;
    let refused = leaf_paced_pull(
        &leaf_ep,
        b_target,
        leaf_node_id,
        &leaf_eth,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused,
        "an exhausted global leech budget must refuse the pull (signed NotFound)"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "admission refusal must precede any upstream spend, got {:?}",
        progress_log(&recorded)?
    );
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "a budget-refused request must not fill B's cache"
    );
    assert_counter(&b_metrics, "node_pull_through_leech_budget_paused_total", 1)?;

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_oversized_upstream_aborts_and_releases_tee() -> Result<()> {
    // #856 step (4): the fused serve opens the upstream pull, reads `total_bytes`
    // from the signed header, and — if it exceeds this node's `max_blob_size_bytes`
    // — signs `BlobTooLarge` (wire `NotFound`), abandons BOTH the upstream pull and
    // the cache tee, and forwards nothing. The regression this guards: a dropped
    // size gate would fuse-serve an over-ceiling blob; a forgotten `tee.abandon()`
    // on this arm would strand the in-flight tee claim for the hash. We assert the
    // refusal, the `blob_too_large` metric, that no voucher was paid upstream
    // (channel opened, zero bytes pulled), and that nothing was cached.
    let payload = vec![0xB1u8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA6);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x6F);
    // 1 MiB ceiling, below the 1.5 MiB blob, so the SIZE gate trips — but the
    // deposit guard (ceiling = min_payment(1 MiB, RATE) = 10 µUSDC) passes against
    // the funded leaf, so we exercise step (4), not the step (1) deposit guard.
    let max_blob_size_bytes = 1024 * 1024;
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
            None,
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
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused,
        "an upstream blob over the size ceiling must be refused (signed NotFound), not fused-served"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "the size gate must abort before any voucher is paid upstream, got {:?}",
        progress_log(&recorded)?
    );
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "an oversized-upstream refusal must not promote the blob into B's cache"
    );
    assert_counter(&b_metrics, "serve_stream_rejected_blob_too_large_total", 1)?;
    // The tee claim was released on the abort arm: a second request for the SAME
    // hash is not wedged on a stranded in-flight entry — it reaches the size gate
    // again and is refused identically (a leaked tee would instead hang/coalesce).
    let retry_sk = fresh_key();
    let retry_node_id = B256::from(*retry_sk.public().as_bytes());
    let (retry_ep, _) = local_endpoint(retry_sk, vec![]).await?;
    let refused_again = leaf_paced_pull(
        &retry_ep,
        b_target,
        retry_node_id,
        &leaf_eth,
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused_again,
        "a repeat request for the same hash must be refused again, not wedged on a stranded tee claim"
    );
    assert_counter(&b_metrics, "serve_stream_rejected_blob_too_large_total", 2)?;

    retry_ep.close().await;
    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_share_ratio_refuses_at_admission() -> Result<()> {
    // #856 step (2): the per-peer share ratio can deny a speculative pull at
    // *admission*, before any upstream byte is pulled — distinct from the
    // mid-stream stall (`window_pull_through_leech_stall_*`) and the global-budget
    // admission refusal (`..._global_budget_exhausted_*`). A peer with zero opening
    // allowance and a 0% share ratio (and no prior service) is over its ceiling on
    // the very first check, so the serve signs `CacheMiss` (wire `NotFound`) and
    // the share-ratio pause fires once, with no upstream spend and nothing cached.
    let payload = vec![0xC2u8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);

    let ab_channel_id = B256::repeat_byte(0xA8);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x8F);
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep, _leech_gov) =
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
            // No opening allowance, no share-ratio growth, global budget off: the peer is
            // immediately over its (zero) ceiling at the first admission poll. These caps
            // satisfy `LeechCaps::new` (a `0` global budget disables the window≤budget
            // cross-check), so the validated constructor is used here.
            Some(
                LeechCaps::new(LeechCapsConfig {
                    max_unrecouped_leech_bytes: Bytes::new(0),
                    initial_allowance_bytes: Bytes::new(0),
                    share_ratio_percent: Percent::new(0),
                })
                .map_err(|e| anyhow::anyhow!("invalid caps: {e}"))?,
            ),
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
        leaf_channel_id,
        hash,
        RATE,
        None,
    )
    .await
    .is_err();
    anyhow::ensure!(
        refused,
        "a peer over its per-peer share ratio must be refused at admission (signed NotFound)"
    );
    anyhow::ensure!(
        progress_log(&recorded)?.is_empty(),
        "the share-ratio admission refusal must precede any upstream spend, got {:?}",
        progress_log(&recorded)?
    );
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "a share-ratio-refused request must not fill B's cache"
    );
    assert_counter(&b_metrics, "node_pull_through_share_ratio_paused_total", 1)?;

    leaf_ep.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    task_b.await?;
    Ok(())
}

/// Two concurrent misses to ONE provider must both be delivered — the contract
/// `stream_fetch_shared` documents, and which both node pull paths broke by building a
/// fresh `ChannelLedger` per pull (#1145 review).
///
/// Nothing exotic is staged here. Node B misses two DIFFERENT blobs at once and both rank
/// the same provider first, which is what an ordinary node on a tens-of-nodes network does
/// all day: the cache engine coalesces in-flight pulls BY HASH, so distinct hashes run
/// concurrent `NodeOrigin::fetch` calls, and both take the channel-REUSE fast path and read
/// the same `prior_nonce`.
///
/// With a ledger each, both pulls sign `prior_nonce + 1`. Node A — the real `ClientHandler`,
/// enforcing real nonce monotonicity — accepts the first and rejects the second
/// `StaleNonce`, so one of these two fetches comes back empty. That alone was a bad day;
/// what makes it a money bug is that `StaleNonce` is now a TERMINAL verdict — it wedges the
/// channel (the row is kept for the reclaim sweep, but the provider is suppressed and the
/// loser's ledger desyncs) — so a collision the shared ledger prevents would otherwise strand
/// the deposit. Hence the assertions beyond "both blobs arrived": nothing was retired, and the
/// channel's nonce advanced through EVERY voucher of both pulls on one monotonic sequence.
///
/// Since #1484 the client sends vouchers optimistically and each pull persists the shared
/// ledger's SETTLE-HIGH watermark, so both `record_progress` calls now report the fully
/// advanced cumulative rather than two disjoint sub-watermarks: the evidence of sharing is
/// that the recorded nonce reaches the full four-voucher total (one interval + one closing per
/// 1.5 MiB pull), which two separate ledgers — each capped at nonce 2 and colliding on nonce
/// 1 — could never reach.
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
    let channel_id = B256::repeat_byte(0xC7);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        retired: Arc::clone(&retired),
    }) as Arc<dyn ChannelOpener>;

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
        region_accountant: empty_region_accountant(),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout: Duration::from_secs(20),
            stall_timeout: Duration::from_secs(20),
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            // Reactive mid-pull top-up OFF (#1530): this fixture asserts what a pull
            // does when its channel runs dry, which a self-funding one would hide.
            working_deposit: U256::ZERO,
            reactive_topup_min_ttl: Duration::from_hours(24),
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    });

    // The two misses race, exactly as two cache misses for different blobs do.
    let (first, second) = tokio::join!(
        Origin::fetch(&origin, hash, u64::MAX),
        Origin::fetch(&origin, hash2, u64::MAX),
    );
    let got1 = first
        .map_err(|e| anyhow::anyhow!("first concurrent fetch failed: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "the first concurrent pull returned NOTHING — its voucher collided with the \
                 other pull's on `prior_nonce + 1` and the upstream rejected it StaleNonce"
            )
        })?;
    let got2 = second
        .map_err(|e| anyhow::anyhow!("second concurrent fetch failed: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "the second concurrent pull returned NOTHING — its voucher collided with the \
                 other pull's on `prior_nonce + 1` and the upstream rejected it StaleNonce"
            )
        })?;
    anyhow::ensure!(got1.as_ref() == payload.as_slice(), "blob 1 bytes mismatch");
    anyhow::ensure!(
        got2.as_ref() == payload2.as_slice(),
        "blob 2 bytes mismatch"
    );

    // The collision's real cost: `StaleNonce` is a terminal verdict, so the losing pull
    // would have RETIRED the channel the winner was still streaming on.
    let retired_now = retired.lock().expect("retired lock").clone();
    anyhow::ensure!(
        retired_now.is_empty(),
        "no channel may be retired here — both pulls paid honestly on a live channel, got {retired_now:?}"
    );

    // Both pulls issued through ONE ledger, so the channel's nonces form a single
    // monotonic sequence carrying every voucher from both pulls: two per 1.5 MiB pull
    // (one interval + one closing), four in all. Separate ledgers would each cap at
    // nonce 2 and collide on nonce 1 — which the empty-result / retire checks above
    // already catch. Under the optimistic loop (#1484) both pulls persist the shared
    // settle-high watermark, so the sharing is evidenced by the recorded nonce reaching
    // the full four-voucher total rather than by the two settlements differing (they no
    // longer need to: both observe the same advanced cumulative).
    let entries = recorded.lock().expect("recorded lock").clone();
    anyhow::ensure!(
        entries.len() == 2,
        "expected one recorded settlement per pull, got {entries:?}"
    );
    let top = entries
        .iter()
        .map(|(_, n, ..)| *n)
        .max()
        .unwrap_or(U256::ZERO);
    anyhow::ensure!(
        top == U256::from(4u64),
        "the shared ledger's nonce must carry all four vouchers (two per 1.5 MiB pull); \
         separate ledgers would each cap at nonce 2 and collide: {entries:?}"
    );

    ep_a.close().await;
    task_a.await?;
    Ok(())
}

/// Issue #1481 review items 2/3: a real end-to-end exercise of the WIRED resume path
/// (`fetch_inner`'s retry loop, `crates/client-pull/src/lib.rs`) — not just the
/// `ChannelLedger::reseed` primitive tested in isolation in `client-pull`'s own unit tests.
///
/// This calls [`stream_fetch_shared`], which UNTIL #1530 was the entrypoint the node's
/// cache-miss pulls used (at a fixed `byte_offset == 0`). It no longer is: the daemon's miss
/// leg now drives its own resume loop in `node_origin/resume.rs`, so what this test pins today
/// is the `client-pull`-internal retry that the CLI's `fetch_blob`/bundle-pull path used to
/// rely on; both CLI commands now stream through `open_progressive_pull` and drive their own
/// resume loop instead, so this wrapper's `byte_offset == 0` path is exercised only by
/// `stream_fetch`'s test-only callers, this test included. The node's twin of the same
/// contract — a bundled watermark reseeds rather
/// than terminating — is pinned by `node_origin::resume::tests::an_advancing_bundle_reseeds`
/// and its siblings.
///
/// The conclusion it reaches is unchanged either way: a bundled `StaleNonce` is retried
/// transparently before `pull_verdict` / `voucher_verdict` ever see it, which is why neither
/// classifier needed to change.
///
/// The trigger is genuine, not simulated: two SEPARATE [`ChannelLedger`]s on ONE real channel,
/// driven one after another against a real `ClientHandler` + real `MemoryChannelStateStore`
/// (exactly the harness [`two_concurrent_pulls_to_one_provider_share_the_channel_ledger`] uses,
/// minus the sharing). Pull A's ledger is the first ever voucher on this channel and advances
/// the node's real on-node nonce to 1. Pull B's ledger is FRESH — never saw pull A — exactly a
/// wallet-less delegate that lost its watermark between sessions. Its first voucher collides at
/// nonce 1 and the real `ClientHandler` genuinely rejects `StaleNonce`. Because pull B's
/// rejected voucher recovers to the channel's registered `voucher_signer` (both pulls use the
/// SAME buyer key) and the channel already holds an accepted voucher (from pull A) to report,
/// the node-side gate (task 5) is satisfied and a `WatermarkBundle` rides back on the wire.
///
/// Before the review's fix this came back `Err(UpstreamVoucherRejected)` — a wallet-less
/// client's pull was simply abandoned. This test would have failed against that code; it must
/// pass now, with pull B's caller seeing NOTHING unusual at all: the retry, reseed, and second
/// successful voucher round trip happen entirely inside `client-pull`.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // test setup; a real end-to-end wire scenario, not logic to split
async fn a_stale_nonce_rejection_with_a_bundle_self_heals_over_the_wire() -> Result<()> {
    let payload_a = vec![0xA1u8; 4096];
    let hash_a = Hash::new(&payload_a);
    let payload_b = vec![0xB2u8; 4096];
    let hash_b = Hash::new(&payload_b);
    anyhow::ensure!(hash_a != hash_b, "fixtures must be distinct blobs");

    let (cache_a, _tmp_a) = cache_with_blobs(&[payload_a.as_slice(), payload_b.as_slice()]).await?;
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0xC7);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
        16,
    )?;
    let (ep_a, addr_a) =
        local_endpoint(a_sk, vec![ALPN_PROBE.to_vec(), ALPN_CLIENT.to_vec()]).await?;
    let task_a = spawn_a_server(
        ep_a.clone(),
        handler_a,
        Arc::clone(&a_eth),
        slash_domain(),
        4096,
        RATE,
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(a_id).with_ip_addr(addr_a);
    let deadlines = PullDeadlines::new(Duration::from_secs(20), Duration::from_secs(20))
        .map_err(|e| anyhow::anyhow!("deadlines: {e}"))?;

    let ctx = ChannelContext {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        client_signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    };

    // Pull A: a fresh ledger, first ever voucher on this channel — advances the node's real
    // state to nonce 1.
    let ledger_a = ChannelLedger::new(Cumulative::default());
    let got_a = stream_fetch_shared(
        &ep_b,
        target.clone(),
        &ctx,
        &ledger_a,
        &slash_domain(),
        a_eth.address(),
        *hash_a.as_bytes(),
        0,
        now_us(),
        deadlines,
        0,
        0,
    )
    .await
    .map_err(|e| anyhow::anyhow!("pull A (seeding the channel) failed: {e}"))?;
    anyhow::ensure!(
        got_a.as_ref() == payload_a.as_slice(),
        "pull A bytes mismatch"
    );

    // Pull B: a SEPARATE ledger modelling a wallet-less delegate that correctly tracked the
    // channel's cumulative amount/bytes (e.g. it sums what it has spent locally) but whose
    // NONCE counter specifically desynced — nonce 0 while amount/bytes already match pull A's
    // real ending state. This is deliberate, not an oversight: seeding a TOTALLY fresh
    // `Cumulative::default()` here would undersign relative to `delta_bytes` (the real
    // channel's `last_amount` already exceeds an amount computed from a zero baseline) and hit
    // the UNRELATED pre-existing hard-fail underpayment path in
    // `crates/node/src/handlers/client/voucher.rs` (`anyhow::bail!("voucher underpays…")`)
    // before ever reaching the nonce check this test exists to exercise. Matching amount/bytes
    // while leaving the nonce stale isolates the ONE thing this test is about: a genuine
    // `StaleNonce` that reaches `apply_voucher`, gets gated, and comes back with a bundle.
    let seed = ledger_a.settlement();
    let ledger_b = ChannelLedger::new(Cumulative {
        nonce: U256::ZERO,
        bytes: seed.bytes,
        amount: seed.amount,
    });
    let got_b = stream_fetch_shared(
        &ep_b,
        target.clone(),
        &ctx,
        &ledger_b,
        &slash_domain(),
        a_eth.address(),
        *hash_b.as_bytes(),
        0,
        now_us(),
        deadlines,
        0,
        0,
    )
    .await
    .map_err(|e| {
        anyhow::anyhow!("pull B must self-heal a bundled StaleNonce transparently: {e}")
    })?;
    anyhow::ensure!(
        got_b.as_ref() == payload_b.as_slice(),
        "pull B must return the FULL blob, unchanged from what byte_offset == 0 promises — a \
         retry that jumped the wire byte_offset to the channel's cumulative bytes_delivered \
         would truncate this"
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok(())
}

// The negative twin of the test above: a mid-stream `StaleNonce` with NO bundle (the
// pre-#1481 shape, still what a genuinely non-gated or unverifiable rejection looks like)
// stays terminal — the existing hand-rolled-upstream tests already cover this
// (`pull_against_a_voucher_rejecting_upstream` and friends; `serve_then_reject_voucher`
// always sends `bundle: None`), so it is not duplicated here.

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
    let channel_id = B256::repeat_byte(0x5C);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_some_and(|b| b.as_ref() == payload.as_slice()),
        "first fetch must deliver the blob"
    );
    anyhow::ensure!(probes.load(Ordering::SeqCst) == 1, "first fetch must probe");
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_some_and(|b| b.as_ref() == payload.as_slice()),
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

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
    let channel_id = B256::repeat_byte(0x7E);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: b_buyer2,
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    // A 300ms positive-cache TTL: short enough that a real sleep past it is not a
    // test-suite hazard, unlike the production 15s (anchored on `Instant`, so
    // `tokio::time::pause` cannot fast-forward it).
    let origin = build_origin_with_probe_caches(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
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
    );

    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_some_and(|b| b.as_ref() == payload.as_slice()),
        "first fetch must deliver the blob"
    );
    anyhow::ensure!(probes.load(Ordering::SeqCst) == 1, "first fetch must probe");

    // Sleep well past the 300ms TTL — generous margin, as the negative-cache
    // TTL-inversion tests use.
    tokio::time::sleep(Duration::from_millis(900)).await;

    let second = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_some_and(|b| b.as_ref() == payload.as_slice()),
        "second fetch must deliver the blob"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 2,
        "a fetch past the TTL must probe again — the entry is gone, got {} probes",
        probes.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;
    assert_counter(&b_metrics, "probe_cache_hits_total", 0)?;

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok(())
}

/// The request's `namespace_id` is load-bearing on the progressive client-serve leg
/// (#1401): `open_progressive_pull(hash, ns)` must route the directory fallback on
/// `ns`, so an authorized origin resolves under its published namespace and NOT under
/// `NO_NAMESPACE`. The directory here is keyed ONLY under a non-zero namespace, so:
///   - a pull under `NO_NAMESPACE` (0) resolves no origin (the fallback finds nothing),
///   - a pull under that namespace resolves the origin and opens the upstream.
/// A regression that dropped the namespace argument (routing everything as
/// `NO_NAMESPACE`, as the leg did before this change) would flip the first assertion
/// from `None` to `Some` and fail here — which the prior symmetric-`ZERO` tests could
/// not catch.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_progressive_pull_routes_the_fallback_on_the_request_namespace() -> Result<()> {
    const NS: u64 = 7; // any non-zero namespace; the directory is keyed only under it.
    let payload = vec![0x4Bu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client. -----------------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0x4B);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    // Prime B's address book with A's endpoint so subsequent NodeId-only dials
    // (channel open + stream) resolve — same priming as `a_fetch_past_the_ttl`.
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
    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    // Directory keyed ONLY under namespace NS (no `NO_NAMESPACE` entry).
    let origin = build_origin_with_probe_caches(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
        NegativeProbeCache::new(),
        PositiveProbeCache::new(),
        U256::from(NS),
        // Reactive mid-pull top-up off; only the #1530 tests turn it on.
        U256::ZERO,
    );

    // Negative control FIRST (before any pull warms the hash-keyed probe cache):
    // a `NO_NAMESPACE` pull finds no directory origin and no cached candidate, so
    // it resolves nothing. This is the assertion a namespace-dropping regression
    // would break.
    let no_ns = tokio::time::timeout(
        Duration::from_secs(20),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("NO_NAMESPACE open_progressive_pull never returned"))?;
    anyhow::ensure!(
        no_ns.is_err(),
        "a NO_NAMESPACE progressive pull must resolve no authorized origin — the \
         directory has nothing under namespace 0"
    );

    // The request's namespace routes the fallback to the authorized origin and the
    // upstream opens.
    let opened = tokio::time::timeout(
        Duration::from_secs(20),
        origin.open_progressive_pull(hash, U256::from(NS)),
    )
    .await
    .map_err(|_| anyhow::anyhow!("namespaced open_progressive_pull never returned"))?;
    anyhow::ensure!(
        opened.is_ok(),
        "a pull under the published namespace must resolve the authorized origin and \
         open the upstream"
    );
    drop(opened); // no bytes forwarded — nothing to settle on drop.

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
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
        channel_id: B256::repeat_byte(0x3B),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;

    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![n0_dht, n1_dht, n2_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // Fetch #1: cold path. Discovers, probes, and ranks all three; the whole
    // MAX_PROVIDER_ATTEMPTS budget is spent refusing, and the ranked list is
    // cached at `probe_and_rank`'s tail regardless of the miss.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_none(),
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
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_none(),
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
        .map_err(|e| anyhow::anyhow!("third fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        third.is_none(),
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

    ep_b.close().await;
    ep_n0.close().await;
    ep_n1.close().await;
    ep_n2.close().await;
    task_n0.abort();
    task_n1.abort();
    task_n2.abort();
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
    let channel_id = B256::repeat_byte(0x1B);

    let n_dht = DhtNodeId::from_bytes(*n_id.as_bytes());
    let h_dht = DhtNodeId::from_bytes(*h_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(n_dht, n_eth.address());
    addr_map.insert(h_dht, h_eth.address());

    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;

    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![n_dht, h_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // Fetch #1: cold path (cache empty). H is undiscoverable, so only N is
    // probed and cached — a SINGLE cached candidate, fewer than
    // `MAX_PROVIDER_ATTEMPTS`. N refuses, so the fetch is a clean miss.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_none(),
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
    let store_h = Arc::new(MemoryChannelStateStore::new());
    store_h.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_h as Arc<dyn ChannelStateStore>,
        h_rate,
        &domains,
        0,
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
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_some_and(|b| b.as_ref() == payload.as_slice()),
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

    ep_b.close().await;
    ep_n.close().await;
    ep_h.close().await;
    task_n.abort();
    task_h.abort();
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
    let channel_id = B256::repeat_byte(0x9D);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![n_dht, a_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // Fetch #1: cold path. N is tried first (cheaper rate), refuses NotFound
    // (negative-cached for `REFUSAL_SUPPRESSION_TTL`), and the loop falls
    // through to A, which delivers. Both are probed once and the ranked [N, A]
    // pair is cached at `probe_and_rank`'s tail.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_some_and(|b| b.as_ref() == payload.as_slice()),
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
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_some_and(|b| b.as_ref() == payload.as_slice()),
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

    ep_b.close().await;
    ep_n.close().await;
    ep_a.close().await;
    task_n.abort();
    task_a.abort();
    Ok(())
}

/// Task 6 (#1165): the window-paced path shares the probe cache with the buffered one — it
/// must, because it shares the chokepoint that fills it (`probe_and_rank`). A path that
/// populates the cache and never reads it pays the write cost for someone else's benefit.
///
/// `Origin::fetch` (buffered) runs first and writes the cache; `open_progressive_pull`
/// (window-paced) for the SAME hash must then reuse it — no second probe at the wire — while
/// still delivering the payload.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_progressive_pull_reuses_a_probe_cache_entry_written_by_a_buffered_fetch() -> Result<()> {
    let payload = vec![0x1Bu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client, counting probes. ------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0x1B);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
    // — reset so the counter below measures only the pulls under test.
    probes.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let (origin, _recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        channel_id,
        &b_buyer,
        &local_rep,
        &b_metrics,
        providers,
        addr_map,
    );

    // The buffered fetch: cold path. Probes A once and writes the probe cache at
    // `probe_and_rank`'s tail.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("buffered fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_some_and(|b| b.as_ref() == payload.as_slice()),
        "the buffered fetch must deliver the blob"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 1,
        "the buffered fetch must probe"
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    // The window-paced pull for the SAME hash must hit the cache the buffered
    // fetch just wrote and send NO new probe.
    let (header, mut pull) = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("open_progressive_pull never returned"))?
    .map_err(|miss| {
        anyhow::anyhow!("expected an open against the cached candidate, got {miss:?}")
    })?;
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 1,
        "the progressive pull re-probed A — the probe cache saved nothing, which is the \
         entire point of Task 6 (#1165), got {} probes",
        probes.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    // ONE orchestration per call, however many candidate lists it walks — same
    // contract as the buffered path.
    assert_counter(&b_metrics, "node_pull_attempts_total", 2)?;

    let mut wire = Vec::new();
    while let Some(chunk) = pull
        .next_chunk()
        .await
        .map_err(|e| anyhow::anyhow!("next_chunk: {e}"))?
    {
        wire.extend_from_slice(&chunk);
    }
    let decoded = decode_bao_whole(hash, header.total_bytes, &wire).ok_or_else(|| {
        anyhow::anyhow!("the progressive pull delivered a stream that did not verify")
    })?;
    anyhow::ensure!(
        decoded == payload,
        "the progressive pull delivered the wrong bytes"
    );
    pull.finish(TeeVerdict::Verified)
        .await
        .map_err(|e| anyhow::anyhow!("pull finish: {e}"))?;

    ep_b.close().await;
    ep_a.close().await;
    task_a.await?;
    Ok(())
}

/// The window-paced twin of
/// [`a_partial_cached_budget_falls_through_to_the_cold_path_and_meters_once`]: a probe-cache
/// hit whose cached candidates all fail must fall through to the cold path WITHIN THE SAME
/// `open_progressive_pull` call, and that call must still meter `node_pull_attempts_total`
/// exactly once (#1223 review).
///
/// `open_progressive_pull` carries its own copy of the hit → shared budget → cold
/// fall-through flow, including its own `if !attempt_metered` guard — and until this test,
/// nothing drove that path: the reuse test above delivers straight from the hit, and the
/// buffered partial-budget test never touches the window path. A regression that
/// double-meters (or a fall-through that stops working) in `open_progressive_pull` alone
/// left the suite green.
///
/// Same fixture as the buffered twin: provider N refuses every open but answers probes
/// honestly (cached after the first fetch, fails again on the hit); provider H is
/// unreachable during the first fetch, then comes online quoting a far cheaper rate so the
/// cold fall-through ranks it first and opens from it without retrying N.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_window_pull_with_a_partial_cached_budget_falls_through_cold_and_meters_once()
-> Result<()> {
    let payload = vec![0x6Eu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    // --- Provider N: reachable, honest at probe, refuses with `InternalError` —
    //     reported node-fault, so NOT negative-cached and NOT wedged, exactly like
    //     the buffered twin's refuser. ---------------------------------------------
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
    //     undiscoverable until it comes online after the buffered fetch. ----------
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
    // — reset so the counters below measure only the calls under test.
    probes_n.store(0, Ordering::SeqCst);
    streams_n.store(0, Ordering::SeqCst);

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let b_metrics = Arc::new(Metrics::new());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0x6E);

    let n_dht = DhtNodeId::from_bytes(*n_id.as_bytes());
    let h_dht = DhtNodeId::from_bytes(*h_id.as_bytes());
    let mut addr_map = HashMap::new();
    addr_map.insert(n_dht, n_eth.address());
    addr_map.insert(h_dht, h_eth.address());

    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;

    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![n_dht, h_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // The buffered fetch: cold path (cache empty). H is undiscoverable, so only N
    // is probed and cached — a SINGLE cached candidate, fewer than
    // `MAX_PROVIDER_ATTEMPTS`. N refuses, so the fetch is a clean miss.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("buffered fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_none(),
        "H is unreachable and N refuses; the buffered fetch must miss"
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;

    // --- Bring H online between the two calls: a real, healthy upstream holding
    //     the blob, quoting a far cheaper rate so it always outranks N once both
    //     are probed fresh. ---------------------------------------------------------
    let (cache_h, hash_h, _tmp_h) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_h == hash, "fixture hash mismatch");
    let store_h = Arc::new(MemoryChannelStateStore::new());
    store_h.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_h as Arc<dyn ChannelStateStore>,
        h_rate,
        &domains,
        0,
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
    // Again, the priming dial is itself a real probe — reset before the call
    // under test.
    probes_h.store(0, Ordering::SeqCst);

    // The window-paced pull: cache hit on N alone. The cached phase spends 1 of
    // the 3-attempt budget on N (refused again), leaving budget > 0, so the call
    // falls through — WITHIN THIS CALL — to a fresh lookup that discovers both N
    // and H, ranks H first (cheaper rate), and opens from H without retrying N.
    let (header, mut pull) = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("open_progressive_pull never returned"))?
    .map_err(|miss| anyhow::anyhow!("expected an open via the cold fallthrough, got {miss:?}"))?;
    anyhow::ensure!(
        streams_n.load(Ordering::SeqCst) == 2,
        "N must be opened-against exactly twice total (once per call) — the cold path must \
         not retry it once H (ranked first) opens, got {}",
        streams_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        probes_n.load(Ordering::SeqCst) == 2,
        "N must be re-probed by the fresh cold-path lookup (the cache hit itself never \
         probes), got {}",
        probes_n.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        probes_h.load(Ordering::SeqCst) == 1,
        "H must be probed exactly once, by the cold-path fallthrough, got {}",
        probes_h.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    // THE property under test: one orchestration per call, however many candidate
    // lists it walks THIS TIME — 2, not 3. A regressed `if !attempt_metered` guard
    // on the window path would double-count this call's cold fallthrough.
    assert_counter(&b_metrics, "node_pull_attempts_total", 2)?;

    // The open must actually be H delivering the payload, not a dangling header.
    let mut wire = Vec::new();
    while let Some(chunk) = pull
        .next_chunk()
        .await
        .map_err(|e| anyhow::anyhow!("next_chunk: {e}"))?
    {
        wire.extend_from_slice(&chunk);
    }
    let decoded = decode_bao_whole(hash, header.total_bytes, &wire).ok_or_else(|| {
        anyhow::anyhow!("the window-paced pull delivered a stream that did not verify")
    })?;
    anyhow::ensure!(
        decoded == payload,
        "the window-paced pull delivered the wrong bytes"
    );
    pull.finish(TeeVerdict::Verified)
        .await
        .map_err(|e| anyhow::anyhow!("pull finish: {e}"))?;

    ep_b.close().await;
    ep_n.close().await;
    ep_h.close().await;
    task_n.abort();
    task_h.abort();
    Ok(())
}

/// The window-paced twin of
/// [`cached_candidates_and_the_cold_path_share_one_attempt_budget`]: the FULL-exhaustion
/// contract on `open_progressive_pull`, which carries its own copy of the exhaustion arm
/// (`node_origin.rs`: the `budget == 0 → return None` short-circuit and the `invalidate`
/// that precedes it) separate from `Origin::fetch`'s (#1223 review).
///
/// The partial-budget window twin above only ever leaves budget > 0, so the window path's
/// `budget == 0` early-return and its `probe_cache.invalidate(&target)` were BOTH untested:
/// deleting either left the whole suite green, exactly the blind spot a third fetch closed
/// on the buffered side. This drives them:
///
/// - Call #2 (the hit) must spend the whole 3-attempt budget on the three cached refusers
///   and END there — NOT reach `discover` for three more opens. Proven at the wire by the
///   probe counter staying at 3 (the exhaustion short-circuit fires before any fresh lookup)
///   and by exactly three more stream opens, not six.
/// - Call #3 must go COLD: the entry call #2 disproved has to have been invalidated, so the
///   third call re-probes all three at the wire (probe counter 3 → 6). Without the
///   `invalidate`, it would hit the same dead list a second time and never re-probe.
///
/// Same fixture as the buffered budget test: three providers reachable and honest at probe,
/// refusing every open with `InternalError` (a reported node-fault, so NOT negative-cached
/// and NOT wedged — which is what keeps all three in the cache for the hit to spend budget
/// on).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn a_window_pull_shares_one_attempt_budget_and_invalidates_on_exhaustion() -> Result<()> {
    let payload = vec![0x7Cu8; 4096];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    // Two wire counters shared across all three servers: probes prove the
    // exhaustion short-circuit (no fresh lookup) and the invalidate (re-probe on
    // call #3); streams prove the shared budget (three opens per call, not six).
    let probes = Arc::new(AtomicUsize::new(0));
    let streams = Arc::new(AtomicUsize::new(0));

    // --- Three providers: reachable, honest at probe, refuse the open with
    //     `InternalError` — reported node-fault, so NOT negative-cached and NOT
    //     wedged, so all three survive in the cache for call #2 to walk. ----------
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
    // servers — reset so the counters below measure only the calls under test.
    probes.store(0, Ordering::SeqCst);
    streams.store(0, Ordering::SeqCst);

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
        channel_id: B256::repeat_byte(0x7C),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;

    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        vec![n0_dht, n1_dht, n2_dht],
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // Call #1: cold path (cache empty). Discovers, probes, and ranks all three;
    // the whole budget is spent opening-and-refused, and the ranked list is cached
    // at `probe_and_rank`'s tail regardless of the miss.
    let first = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("call #1 open_progressive_pull never returned"))?;
    anyhow::ensure!(
        first.is_err(),
        "all three providers refuse; the first window pull must miss"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 3,
        "expected all three providers probed exactly once on the cold path, got {}",
        probes.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        streams.load(Ordering::SeqCst) == 3,
        "the cold path must open-against all three within budget, got {}",
        streams.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;
    assert_counter(&b_metrics, "node_pull_attempts_total", 1)?;

    // Call #2: the probe-cache hit walks the SAME three cached candidates. It must
    // spend exactly the fetch-wide budget of 3 opening-and-refused, then hit the
    // window path's `budget == 0` short-circuit — WITHOUT re-probing and WITHOUT a
    // fresh lookup for three more opens. Three more streams (6 total), not six.
    let second = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("call #2 open_progressive_pull never returned"))?;
    anyhow::ensure!(
        second.is_err(),
        "the cached candidates all refuse again; the second window pull must miss too"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 3,
        "the exhausted hit must NOT reach a fresh lookup — no re-probe expected, got {} probes",
        probes.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        streams.load(Ordering::SeqCst) == 6,
        "the hit must open-against exactly the 3-attempt budget (6 total), not hand a fresh \
         lookup three more, got {}",
        streams.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    // Call #3: call #2 disproved the cached entry by spending the whole budget on
    // it, so the window path must have INVALIDATED it — none of the three refusers
    // is negative-cached or wedged, so only the invalidate stands between this call
    // and a second hit on the dead list. It must go COLD: a fresh lookup that
    // re-probes all three at the wire.
    let third = tokio::time::timeout(
        Duration::from_secs(10),
        origin.open_progressive_pull(hash, U256::ZERO),
    )
    .await
    .map_err(|_| anyhow::anyhow!("call #3 open_progressive_pull never returned"))?;
    anyhow::ensure!(
        third.is_err(),
        "all three providers still refuse; the third window pull must miss too"
    );
    anyhow::ensure!(
        probes.load(Ordering::SeqCst) == 6,
        "the third call must re-probe all three — the exhausted entry was disproved by real \
         opens and must not survive to hit again, got {} probes",
        probes.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        streams.load(Ordering::SeqCst) == 9,
        "the cold re-walk opens all three once more (9 total), got {}",
        streams.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;
    // Still one orchestration per call: cold, hit-exhausted, cold again.
    assert_counter(&b_metrics, "node_pull_attempts_total", 3)?;

    ep_b.close().await;
    ep_n0.close().await;
    ep_n1.close().await;
    ep_n2.close().await;
    task_n0.abort();
    task_n1.abort();
    task_n2.abort();
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
        channel_id: B256::repeat_byte(0x2F),
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
        0,
    );

    // Fetch #1: cold path. N is probed, ranked, cached — and its refusal
    // negative-caches (N, hash) for the full TTL, stranding the fresh positive
    // entry with no usable provider.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(first.is_none(), "N refuses; the first fetch must miss");
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
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_none(),
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

    ep_b.close().await;
    ep_n.close().await;
    task_n.abort();
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
/// then wedges its channel on a pull for hash1 (fetch #2: A rejects the closing voucher
/// with `StaleNonce` → `OurDeadChannel` → provider-wide suppression until channel expiry).
/// (A, hash2) is never negative-cached, so when H goes offline and fetch #3 hits the hash2
/// entry, ONLY the wedged filter stands between A and being handed back a channel that
/// cannot pay. The wire-level assertion is A's stream counter: still exactly one open ever.
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
    //     the closing voucher with `StaleNonce` — the wedge trigger. -------------
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
        VoucherRejectReason::StaleNonce,
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
    let channel_id = B256::repeat_byte(0x3D);
    let store_h = Arc::new(MemoryChannelStateStore::new());
    store_h.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_h as Arc<dyn ChannelStateStore>,
        h_rate,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_multi_hash(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        &[hash1, hash2],
        buyer,
        &local_rep,
        &b_metrics,
        &empty_region_accountant(),
        &[a_dht, h_dht],
        addr_map,
        // Short pull/stall deadlines: fetch #3 dials the offline H at an address
        // it KNOWS (unlike the fallthrough tests' never-primed identities, a dead
        // UDP addr times out rather than refusing), and that wait is bounded by
        // these. The pulls that matter complete in milliseconds.
        Duration::from_secs(3),
        Duration::from_secs(3),
    );

    // Fetch #1 (hash2): cold path. Both are probed and cached in the hash2 entry;
    // H (cheaper) ranks first and delivers, so A is never pulled — no negative
    // entry, no wedge, just a live cached candidate.
    let first = Origin::fetch(&origin, hash2, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_some_and(|b| b.as_ref() == payload2.as_slice()),
        "fetch #1 must deliver hash2 from H"
    );
    anyhow::ensure!(
        streams_a.load(Ordering::SeqCst) == 0,
        "A must not be pulled for hash2 while H (ranked first) delivers, got {}",
        streams_a.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_channel_wedged_total", 0)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 1)?;

    // Fetch #2 (hash1): H is tried first and honestly refuses (its cache holds
    // only payload2); A then serves payload1 but rejects the closing voucher —
    // `OurDeadChannel`, wedging A provider-wide until its channel expires.
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
    anyhow::ensure!(
        streams_a.load(Ordering::SeqCst) == 1,
        "A must be pulled exactly once (the hash1 wedge), got {}",
        streams_a.load(Ordering::SeqCst)
    );
    assert_counter(&b_metrics, "node_pull_channel_wedged_total", 1)?;
    assert_counter(&b_metrics, "node_pull_voucher_rejected_total", 1)?;
    let probes_a_after_wedge = probes_a.load(Ordering::SeqCst);

    // --- Take H offline: the hash2 entry now reads [H (dead), A (wedged)], and
    //     only the wedged filter keeps fetch #3 from handing A's dead channel
    //     back. ---------------------------------------------------------------------
    ep_h.close().await;
    task_h.abort();

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
    // THE property under test, at the wire: A's one stream ever is the hash1
    // wedge. A second open here is the hit path handing back the dead channel.
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
    assert_counter(&b_metrics, "node_pull_channel_wedged_total", 1)?;
    // The entry WAS consulted (H survived the filters), so fetch #3 is a hit.
    assert_counter(&b_metrics, "probe_cache_hits_total", 1)?;
    assert_counter(&b_metrics, "probe_cache_misses_total", 2)?;

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
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

impl OriginDirectory for MutableOriginDirectory {
    fn lookup_origins(&self, namespace_id: U256) -> Vec<DhtNodeId> {
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
                    let _ = handler.accept(conn).await;
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
    let channel_id = B256::repeat_byte(0xA7);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
        b_buyer.address(),
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
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
        store_a as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
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
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::new(Mutex::new(Vec::new())),
        retired: Arc::new(Mutex::new(Vec::new())),
    }) as Arc<dyn ChannelOpener>;

    // Provisioned inline (the shared builders hardcode `ConfigStakerSet` /
    // `StaticOriginDirectory`, which cannot be mutated mid-test). A default 15s
    // positive-cache TTL keeps the fetch #1 entry live through the ejection and
    // fetch #2, so only the `is_active` re-check can drop it.
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
        region_accountant: empty_region_accountant(),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout: Duration::from_secs(20),
            stall_timeout: Duration::from_secs(20),
            max_blob_size_bytes: 0,
            max_rate_per_mb: 0,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            // Reactive mid-pull top-up OFF (#1530): this fixture asserts what a pull
            // does when its channel runs dry, which a self-funding one would hide.
            working_deposit: U256::ZERO,
            reactive_topup_min_ttl: Duration::from_hours(24),
            // Short, so a post-top-up settle wait cannot dominate a test's wall clock.
            // The fixtures accept the resumed open immediately, so the budget is only
            // ever spent when a test deliberately withholds settlement.
            event_poll_interval: Duration::from_millis(50),
            lookup: decdn_node::dht::LookupConfig::default(),
            own_region: None,
        },
        ledgers: Arc::new(decdn_node::buyer_ledgers::BuyerLedgers::default()),
        wedged_providers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    });

    // Fetch #1: cold path. `find_providers` is empty (no routing entries), so the
    // directory fallback returns [A]; A is probed once, cached, and delivers.
    let first = Origin::fetch(&origin, hash, u64::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("first fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        first.is_some_and(|b| b.as_ref() == payload.as_slice()),
        "first fetch must deliver the blob from A"
    );
    anyhow::ensure!(
        a_probes.load(Ordering::SeqCst) == 1,
        "A must be probed exactly once on the cold path, got {}",
        a_probes.load(Ordering::SeqCst)
    );
    anyhow::ensure!(
        a_streams.load(Ordering::SeqCst) == 1,
        "A must be pulled exactly once on the cold path, got {}",
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
        .map_err(|e| anyhow::anyhow!("second fetch: {e}"))?
        .collect_to_bytes()
        .await?;
    anyhow::ensure!(
        second.is_none(),
        "second fetch must miss — the only cached provider was ejected inside the TTL"
    );
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

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
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

/// A bao verified-stream for `payload` starting at `byte_offset`, matching what a
/// real upstream serves for a resumed request.
///
/// [`honest_bao_wire`]'s resuming twin. The proof is anchored to whole chunk groups,
/// so the encoding is a NEW range encoding with its own root->offset proof path —
/// not a suffix of the offset-0 one. That is precisely why a resumed leg's wire cost
/// cannot be derived by subtracting from the whole-blob cost, and why
/// `content_paid_frontier` re-anchors per leg.
fn honest_bao_wire_from(payload: &[u8], byte_offset: u64) -> Result<Vec<u8>> {
    let hash = Hash::new(payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        payload,
        decdn_cache::range_pull::IROH_BLOCK_SIZE,
    );
    let aligned = decdn_cache::range_pull::align_range(byte_offset, 0, total_bytes)?;
    // `encode_verified_range` takes the WINDOW's plaintext, not the whole blob — the
    // offset-0 twin gets away with passing `payload` only because there the window is
    // the whole blob.
    let start = usize::try_from(aligned.fetch_start()).unwrap_or(usize::MAX);
    let end = usize::try_from(aligned.fetch_end()).unwrap_or(usize::MAX);
    let window = payload
        .get(start..end)
        .ok_or_else(|| anyhow::anyhow!("aligned window {start}..{end} outside the payload"))?;
    let combined = decdn_cache::range_pull::encode_verified_range(
        *hash.as_bytes(),
        &aligned,
        window,
        bytes::Bytes::from(ob.data),
    )?;
    Ok(combined
        .get(8..)
        .ok_or_else(|| anyhow::anyhow!("combined encoding shorter than its header"))?
        .to_vec())
}

/// The on-chain deposit an upstream can see, shared with the buyer's opener.
///
/// A real `PaymentChannel` rejects a voucher whose cumulative `amount` exceeds the
/// escrowed deposit (`AmountExceedsDeposit` -> `VoucherRejectReason::InsufficientDeposit`),
/// and a real `topUp` raises that ceiling. Modelling it as one shared cell is what
/// makes the round trip real here: [`FundingOpener::top_up_channel`] raises the same
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
/// upstream fixture reads (see [`SharedDeposit`]), and `top_up_channel` raises that
/// cell and logs the call. `funds` is what a test flips to model the two ways a
/// real top-up can decline to add headroom — a reverted transaction, and a
/// concurrent proactive refill already holding the provider's slot.
#[derive(Debug)]
struct FundingOpener {
    channel_id: B256,
    token: Address,
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
    /// Every `top_up_channel(provider, target)` in order — the test's view of what
    /// the pull tried to fund.
    topups: Arc<Mutex<Vec<(Address, U256)>>>,
    /// Whether a top-up actually adds headroom. `false` models a refusal.
    funds: bool,
}

#[async_trait]
impl ChannelOpener for FundingOpener {
    async fn open_or_reuse_channel(
        &self,
        provider_addr: Address,
        _deposit_hint: U256,
        _budget: Duration,
    ) -> Result<ChannelContext> {
        let recorded = self
            .recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?;
        let (prior_nonce, prior_bytes_delivered, prior_amount) = recorded
            .iter()
            .rev()
            .find(|(provider, ..)| *provider == provider_addr)
            .map_or((U256::ZERO, U256::ZERO, U256::ZERO), |(_, n, b, a)| {
                (*n, *b, *a)
            });
        drop(recorded);
        Ok(ChannelContext {
            channel_id: self.channel_id,
            token: self.token,
            deposit: read_deposit(&self.deposit)?,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: self.voucher_domain.clone(),
            prior_nonce,
            prior_bytes_delivered,
            prior_amount,
            client_binding: None,
        })
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        channel_id: B256,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
        anyhow::ensure!(
            channel_id == self.channel_id,
            "record_progress channel_id {channel_id} != opened channel {}",
            self.channel_id
        );
        self.recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?
            .push((provider_addr, nonce, bytes_delivered, amount));
        Ok(())
    }

    fn retire_channel(&self, _provider_addr: Address, _channel_id: B256) -> Result<bool> {
        Ok(false)
    }

    async fn top_up_channel(&self, provider_addr: Address, target_deposit: U256) -> Result<U256> {
        self.topups
            .lock()
            .map_err(|_| anyhow::anyhow!("topups lock poisoned"))?
            .push((provider_addr, target_deposit));
        let mut deposit = self
            .deposit
            .lock()
            .map_err(|_| anyhow::anyhow!("deposit lock poisoned"))?;
        // Restore SPENDABLE HEADROOM to the target, the same semantics the real
        // `BuyerChannelService::top_up_channel` implements via `refill_amount(deposit,
        // last_amount, target, target)`. A double that read the raw deposit instead
        // would refuse to fund a channel sitting AT the target and fully spent —
        // which is precisely the state this leg exists to rescue (#1600 review).
        let spent = self
            .recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("recorded lock poisoned"))?
            .iter()
            .rev()
            .find(|(provider, ..)| *provider == provider_addr)
            .map_or(U256::ZERO, |(_, _, _, amount)| *amount);
        let remaining = deposit.saturating_sub(spent);
        if self.funds && target_deposit > remaining {
            *deposit = deposit.saturating_add(target_deposit.saturating_sub(remaining));
            // The escrow the upstream can see rises with it — a real `topUp` raises one
            // number, and the buyer's belief and the seller's gate are both views of it.
            *self
                .ceiling
                .lock()
                .map_err(|_| anyhow::anyhow!("ceiling lock poisoned"))? = *deposit;
        }
        Ok(*deposit)
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
async fn serve_with_deposit_ceiling(
    conn: Connection,
    eth: &Arc<PrivateKeySigner>,
    slash: &Eip712Domain,
    payloads: &[Arc<Vec<u8>>],
    rate: u64,
    deposit: &SharedDeposit,
    resume_delay: Duration,
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
    let resp = signed_response(&req, eth, slash, rate, total_bytes, None)?;
    write_frame(
        &mut send,
        &encode_message(&ClientMessage::StreamResponse(resp))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write response: {e}"))?;

    // Serve the RESUME leg (`byte_offset > 0`) slowly, AFTER the response is on the
    // wire so the delay lands in the chunk stream (`pull_to_sink`), not the open. The
    // paid-wait accounting must count this as the upstream serving bytes, not as our
    // funding wait: it is the exact time `PulledBlob::paid_wait` must NOT absorb
    // (#1602). Zero for every test but the delivery-speed regression.
    if req.byte_offset > 0 && !resume_delay.is_zero() {
        tokio::time::sleep(resume_delay).await;
    }

    let wire = honest_bao_wire_from(payload, req.byte_offset)?;
    let mut unvouchered: u64 = 0;
    for chunk in wire.chunks(CHUNK_SIZE) {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::ChunkData(ChunkData::new(chunk.to_vec())?))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
        unvouchered = unvouchered.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if unvouchered >= MB_BYTES {
            if !settle_voucher(&mut send, &mut recv, deposit).await? {
                // Refused: hold the connection so the buyer reads the rejection frame
                // rather than a transport reset (which would score as unreachable).
                let _ = send.finish();
                conn.closed().await;
                return Ok(());
            }
            unvouchered = 0;
        }
    }
    if unvouchered > 0 && !settle_voucher(&mut send, &mut recv, deposit).await? {
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

/// Read the buyer's voucher and either ack it or refuse it `InsufficientDeposit`,
/// exactly as `PaymentChannel` would: the voucher's CUMULATIVE amount is what the
/// deposit has to cover. Returns whether it was acked.
///
/// `bundle: None`, which is what a real node attaches when it holds no prior accepted
/// voucher to echo — and, critically, what keeps `genuine_exhaustion`'s desync
/// carve-out out of the way. A bundle that ADVANCED our nonce would (correctly) route
/// to the reseed path instead of to funding.
async fn settle_voucher(
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
    deposit: &SharedDeposit,
) -> Result<bool> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("read voucher: {e}"))?;
    let (msg, _) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    let ClientMessage::Voucher(v) = msg else {
        anyhow::bail!("deposit-capped upstream: expected a Voucher");
    };
    if U256::from_be_bytes(v.amount) > read_deposit(deposit)? {
        write_frame(
            send,
            &encode_message(&ClientMessage::StreamError(StreamError::VoucherRejected {
                reason: VoucherRejectReason::InsufficientDeposit,
                bundle: None,
            }))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write rejection: {e}"))?;
        return Ok(false);
    }
    write_frame(send, &encode_message(&ClientMessage::VoucherAck)?)
        .await
        .map_err(|e| anyhow::anyhow!("write ack: {e}"))?;
    Ok(true)
}

/// Spawn a provider that answers probes truthfully and serves under a live deposit
/// ceiling (see [`serve_with_deposit_ceiling`]).
fn spawn_deposit_capped_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
    payloads: Vec<Arc<Vec<u8>>>,
    rate: u64,
    deposit: SharedDeposit,
    resume_delay: Duration,
) -> tokio::task::JoinHandle<()> {
    // Same-length blobs only, so the probe's single quoted `total_bytes` is honest
    // for all of them (`two_concurrent_pulls_...` makes the same choice for the
    // same reason).
    let total_bytes = payloads
        .first()
        .map_or(0, |p| u64::try_from(p.len()).unwrap_or(u64::MAX));
    let payloads = Arc::new(payloads);
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
    async fn shutdown(self) {
        self.ep_b.close().await;
        self.ep_a.close().await;
        self.task_a.abort();
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
/// (`alpha = 1.0`, no EWMA blend) against a known `expected_bps`.
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
        channel_id: B256::repeat_byte(0x15),
        token: TOKEN,
        deposit,
        ceiling,
        signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
        topups: Arc::new(Mutex::new(Vec::new())),
        funds: setup.funds,
    });
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());
    let origin = build_origin_with_probe_caches(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        Arc::clone(&opener) as Arc<dyn ChannelOpener>,
        &local_rep,
        &metrics,
        &empty_region_accountant(),
        providers,
        addr_map,
        DEFAULT_TEST_PULL_DEADLINES.0,
        DEFAULT_TEST_PULL_DEADLINES.1,
        total_bytes.saturating_mul(4),
        NegativeProbeCache::new(),
        PositiveProbeCache::new(),
        U256::ZERO,
        U256::from(setup.working_micro_usdc),
    );

    Ok(TopUpFixture {
        origin,
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
/// three voucher intervals of content, so the pull is refused mid-blob with real
/// delivered bytes behind it rather than at the very first voucher.
fn multi_interval_payload() -> Arc<Vec<u8>> {
    let len = usize::try_from(MB_BYTES).unwrap_or(usize::MAX) * 3 + 777;
    Arc::new(
        (0..len)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect(),
    )
}

/// The headline case: a single node→node pull larger than the channel's initial
/// deposit now completes, by funding the shortfall and resuming — the whole point
/// of #1530.
///
/// Before this, the pull ended at the first voucher the initial deposit could not
/// cover, and no retry could help: every candidate opens at the same
/// `deposit_hint`, and a from-zero retry would re-spend the fresh deposit on bytes
/// it had already bought and re-exhaust at the same offset.
#[tokio::test(flavor = "multi_thread")]
async fn a_pull_larger_than_the_initial_deposit_tops_up_once_and_completes() -> Result<()> {
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

    let bytes = got
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("a topped-up pull must deliver the blob, not NotFound"))?;
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
    anyhow::ensure!(
        log.first()
            .is_some_and(|(_, target)| *target == U256::from(200 * RATE)),
        "the top-up must target the WORKING deposit, got {log:?}"
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

    fixture.shutdown().await;
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
/// `alpha = 1.0` drops the EWMA blend, so `score == 0.4·speed + 0.6` with
/// `speed = min(1, bytes_per_sec / expected_bps)`. The upstream is throttled to serve
/// the resume leg over [`SLOW_RESUME`], so the honest `elapsed` spans at least that:
///
/// - CORRECT: `bytes_per_sec ≤ 3 MiB / 3 s = 1 MiB/s`, and at `expected_bps = 2 MiB/s`
///   that is `speed ≤ 0.5`, so `score ≤ 0.8` — comfortably under the bound below.
/// - BUGGED: the ~3 s transfer is folded into `paid_wait`, leaving `elapsed ≈ the
///   pre-top-up leg` (sub-second, no throttle), so `speed` saturates to 1 and the
///   score pins at ≈1.0.
///
/// `> 0.5` — what the other top-up tests assert — passes both, which is exactly why it
/// never caught this.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_post_topup_delivery_scores_slow_not_instant() -> Result<()> {
    /// Long enough that the resumed leg's transfer dominates `elapsed`, so a score that
    /// still reads "fast" can only mean the transfer was wrongly charged to `paid_wait`.
    const SLOW_RESUME: Duration = Duration::from_secs(3);
    /// 2 MiB/s. Picked so the throttled resume (≤1 MiB/s over the whole blob) reads as
    /// `speed ≤ 0.5`, while the bug's sub-second `elapsed` saturates `speed` to 1.
    const EXPECTED_BPS: u64 = 2 * MB_BYTES;

    let payload = multi_interval_payload();
    let mut rep_config = LocalReputationConfig::default();
    // One delivery must move the score to exactly its interaction sample, so the speed
    // term is legible; the default 0.1 EWMA would compress both cases against neutral.
    rep_config.alpha = 1.0;
    rep_config.expected_bps = EXPECTED_BPS;

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
    let bytes = got
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("a topped-up pull must deliver the blob, not NotFound"))?;
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
    // The bound that matters: the throttled resume caps an HONEST `elapsed`'s speed at
    // 0.5, i.e. `score ≤ 0.8`. The bug charges the transfer to `paid_wait`, saturating
    // the speed and pinning the score at ≈1.0, which trips the ceiling.
    let score = fixture.local_rep.score(fixture.provider);
    anyhow::ensure!(
        score > 0.6,
        "a completed delivery must credit correctness+reachability, got {score}"
    );
    anyhow::ensure!(
        score < 0.85,
        "a slow post-top-up delivery must score slow; a score of {score} means the resume \
         leg's transfer was wrongly excluded from `elapsed` (folded into `paid_wait`) — #1602"
    );

    fixture.shutdown().await;
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
        got.collect_to_bytes().await?.is_some(),
        "the pull must have completed for its spend to mean anything"
    );

    let log = progress_log(&fixture.recorded)?;
    let (_, _, billed_wire, _) = log
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

    fixture.shutdown().await;
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

    fixture.shutdown().await;
    Ok(())
}

/// A top-up that lands but adds no headroom ends the pull instead of looping.
///
/// Two real conditions produce this — a reverted transaction, and a concurrent
/// proactive refill already holding the provider's slot — and both must be terminal:
/// retrying on an unchanged deposit exhausts at exactly the same offset, so the loop
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

    fixture.shutdown().await;
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

    fixture.shutdown().await;
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
/// the overshoot ordering corrupts, which is what a regression run trips on. The
/// deterministic pin of the clamp itself is
/// `resume::tests::a_concurrent_pulls_vouchers_cannot_push_the_frontier_past_decoded_bytes`.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pulls_resume_at_their_own_frontier_not_the_channels() -> Result<()> {
    let len = usize::try_from(MB_BYTES).unwrap_or(usize::MAX) * 3 + 777;
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

    // Roughly four wire intervals per blob at RATE, so the combined spend of two
    // concurrent pulls exhausts an initial deposit sized for four — mid-blob, with
    // real delivered-and-paid bytes on BOTH streams behind the rejection.
    let fixture = top_up_fixture_multi(
        vec![Arc::clone(&payload_a), Arc::clone(&payload_b)],
        TopUpSetup::honest(4 * RATE, 400 * RATE, true),
    )
    .await?;

    let (got_a, got_b) = tokio::time::timeout(Duration::from_mins(2), async {
        tokio::join!(
            Origin::fetch(&fixture.origin, hash_a, u64::MAX),
            Origin::fetch(&fixture.origin, hash_b, u64::MAX),
        )
    })
    .await
    .map_err(|_| anyhow::anyhow!("concurrent top-up pulls never finished"))?;

    let bytes_a = got_a
        .map_err(|e| anyhow::anyhow!("fetch A: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("pull A must deliver, not NotFound"))?;
    let bytes_b = got_b
        .map_err(|e| anyhow::anyhow!("fetch B: {e}"))?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("pull B must deliver, not NotFound"))?;
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

    fixture.shutdown().await;
    Ok(())
}

/// The reactive top-up is bounded at ONE per pull, over the wire (#1600 review).
///
/// The pure policy is pinned by `resume::tests::reactive_topups_are_bounded`; this
/// pins the WIRING — the `topups += 1` the loop performs after a landed top-up.
/// Removing that increment is currently invisible to every other test, because the
/// funding double is idempotent against its target (a second call at the same
/// target adds nothing, so `fund` reports no headroom and the loop stops anyway).
/// Here the working deposit is deliberately still too small for the blob, so a loop
/// that did not count would keep funding-and-failing rather than ending after one.
#[tokio::test(flavor = "multi_thread")]
async fn a_working_deposit_that_still_cannot_cover_the_blob_funds_exactly_once() -> Result<()> {
    // ~10 voucher intervals of wire, so the whole blob costs ~10x RATE. The initial
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

    fixture.shutdown().await;
    Ok(())
}
