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
    ChannelState, ChannelStateStore, EPHEMERAL_BINDING_NONCE, MemoryChannelStateStore,
    ProbeSlashData, StreamSlashData, Voucher, bind_node_id_domain, binding_signing_hash,
    signed_to_wire_voucher, slash_judge_domain, voucher_domain,
};
use decdn_node::buyer_channel::{ChannelOpenPending, ChannelOpener};
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::dht::negative_cache::Hash as DhtHash;
use decdn_node::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use decdn_node::dht::{
    ConfigOriginDirectory, ConfigStakerSet, NegativeProbeCache, NodeAddressResolver,
    OriginDirectory, StakerSet, StaticNodeAddressDirectory,
};
use decdn_node::leech_governor::{LeechCaps, LeechCapsConfig, LeechGovernor};
use decdn_node::metrics::Metrics;
use decdn_node::node_origin::{NodeOrigin, NodeOriginConfig, NodeOriginDeps};
use decdn_node::probe_client::probe_once;
use decdn_node::region_accounting::{RegionAccountant, RegionResolver};
use decdn_node::selection::outer_pull_deadline;
use decdn_protocol::client::{
    ChunkData, ClientBinding, ClientMessage, StreamError, StreamRequest, StreamRequestExt,
    StreamResponse, StreamResponseBody, VoucherRejectReason,
};
use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};
use decdn_protocol::{
    ALPN_CLIENT, ALPN_PROBE, CHUNK_SIZE, DEFAULT_VOUCHER_INTERVAL_MB, MB_BYTES, ProbeMessage,
    decode_message, encode_message, encode_stream_request, read_frame, write_frame,
};
use decdn_reputation::{
    LocalReputation, LocalReputationConfig, NetworkReputation, NetworkReputationConfig,
    ObservationBuffer,
};
use iroh::EndpointAddr;
use iroh::endpoint::Connection;

mod support;
use support::{
    HandlerDomains, build_handler_full, cache_with_blob, empty_cache, fresh_key, local_endpoint,
    permissive_limiter, spawn_server,
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
}

#[async_trait]
impl ChannelOpener for StubOpener {
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
        // Resume from the latest persisted watermark for this provider (fresh
        // zeros if none) — the reuse path the #852 fix makes correct.
        let (prior_nonce, prior_bytes_delivered, prior_amount) = recorded
            .iter()
            .rev()
            .find(|(provider, ..)| *provider == provider_addr)
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
            return Err(anyhow::Error::new(ChannelOpenPending {
                provider: provider_addr,
                waited: budget,
            }));
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
    // Hold the connection so the requester reads the response + lingers (ADR
    // 015) before it collapses; spawned per-connection, so this never stalls the
    // accept loop.
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
    obs_buffer: &Arc<ObservationBuffer>,
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
        obs_buffer,
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
    obs_buffer: &Arc<ObservationBuffer>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
) -> (NodeOrigin, Arc<Mutex<Vec<ProgressEntry>>>) {
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(buyer_signer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        obs_buffer,
        metrics,
        region_accountant,
        providers,
        addr_map,
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
    obs_buffer: &Arc<ObservationBuffer>,
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
    }) as Arc<dyn ChannelOpener>;
    build_origin_with_timeout(
        ep_b,
        b_dht,
        hash,
        buyer,
        local_rep,
        obs_buffer,
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
    obs_buffer: &Arc<ObservationBuffer>,
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
        obs_buffer,
        metrics,
        region_accountant,
        providers,
        addr_map,
        Duration::from_secs(20),
        Duration::from_secs(20),
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
    obs_buffer: &Arc<ObservationBuffer>,
    metrics: &Arc<Metrics>,
    region_accountant: &Arc<RegionAccountant>,
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
    stall_timeout: Duration,
    max_blob_size_bytes: u64,
) -> NodeOrigin {
    let mut dir = HashMap::new();
    dir.insert(DhtHash::from_bytes(*hash.as_bytes()), providers);

    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(b_dht))),
        staker_set: Arc::new(ConfigStakerSet::empty()) as Arc<dyn StakerSet>,
        origin_directory: Arc::new(ConfigOriginDirectory::new(dir)) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: b_dht,
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(local_rep),
        obs_buffer: Arc::clone(obs_buffer),
        network_rep: Arc::new(
            NetworkReputation::new(NetworkReputationConfig::default())
                .expect("network reputation config"),
        ),
        rep_cfg: NetworkReputationConfig::default(),
        negative_cache: NegativeProbeCache::new(),
        metrics: Arc::clone(metrics),
        region_accountant: Arc::clone(region_accountant),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout,
            stall_timeout,
            max_blob_size_bytes,
            enable_0rtt: false,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            lookup: decdn_node::dht::LookupConfig::default(),
        },
        acquisition_observer: None,
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

/// #820 end-to-end: a node with prefetch enabled, observing enough `FIND_VALUE`
/// demand for a hash it does not hold, speculatively acquires it via the cache
/// pull-through (DHT/origin discovery → probe → paid pull from an upstream),
/// records the spend into the prefetch ledger, and tags the blob so the serve
/// path can later credit it. Closes AC 7 of #650.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used, clippy::too_many_lines)] // test setup; failures should panic loudly
async fn prefetch_acquire_pulls_and_records_spend() -> Result<()> {
    let payload = vec![0xCDu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // --- Node A: holds the blob; serves probe + client. -----------------------
    let (cache_a, hash_a, _tmp_a) = cache_with_blob(&payload).await?;
    anyhow::ensure!(hash_a == hash, "fixture hash mismatch");
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let channel_id = B256::repeat_byte(0xC1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        channel_id,
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

    // --- Node B: prefetch-enabled, hosts the NodeOrigin in its cache chain. ----
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, _addr_b) = local_endpoint(b_sk, vec![]).await?;
    // Prime B's iroh address cache with A's address (NodeId-only dialing).
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());

    // Prefetch engine (enabled), authorized-origin directory maps the hash to A.
    let prefetch_cfg = decdn_common::config::ResolvedPrefetch {
        enabled: true,
        require_authorized_origin: true,
        budget_usdc_per_hour: 1_000_000,
        find_value_threshold: 2,
        ..Default::default()
    };
    let authorized_dir: Arc<dyn OriginDirectory> =
        Arc::new(ConfigOriginDirectory::new(HashMap::from([(
            DhtHash::from_bytes(*hash.as_bytes()),
            vec![DhtNodeId::from_bytes(*a_id.as_bytes())],
        )])));
    let engine = Arc::new(decdn_node::prefetch::PrefetchEngine::new(
        prefetch_cfg,
        authorized_dir,
    ));
    let observer = Arc::new(decdn_node::prefetch::PrefetchAcquisitionObserver::new(
        Arc::clone(&engine),
        Arc::clone(&b_metrics),
    )) as Arc<dyn decdn_node::node_origin::AcquisitionObserver>;

    // B's NodeOrigin, provisioned with the prefetch acquisition observer.
    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
    }) as Arc<dyn ChannelOpener>;
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(DhtNodeId::from_bytes(
            *b_id.as_bytes(),
        )))),
        staker_set: Arc::new(ConfigStakerSet::empty()) as Arc<dyn StakerSet>,
        origin_directory: Arc::new(ConfigOriginDirectory::new(HashMap::from([(
            DhtHash::from_bytes(*hash.as_bytes()),
            providers,
        )]))) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: DhtNodeId::from_bytes(*b_id.as_bytes()),
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(&local_rep),
        obs_buffer: Arc::clone(&obs_buffer),
        network_rep: Arc::new(
            NetworkReputation::new(NetworkReputationConfig::default())
                .expect("network reputation config"),
        ),
        rep_cfg: NetworkReputationConfig::default(),
        negative_cache: NegativeProbeCache::new(),
        metrics: Arc::clone(&b_metrics),
        region_accountant: empty_region_accountant(),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout: Duration::from_secs(20),
            stall_timeout: Duration::from_secs(20),
            max_blob_size_bytes: 0,
            enable_0rtt: false,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            lookup: decdn_node::dht::LookupConfig::default(),
        },
        acquisition_observer: Some(observer),
    });

    // B's cache with the NodeOrigin last in the chain — exactly how the runtime
    // wires pull-through. `populate` will drive a network pull on a miss.
    let cache_dir_b = tempfile::tempdir()?;
    let cache_b = CacheEngine::open(
        cache_dir_b.path(),
        vec![Arc::new(origin.clone()) as Arc<dyn Origin>],
        16,
    )
    .await?;
    anyhow::ensure!(!cache_b.has(hash).await?, "B should start without the blob");

    engine.provision_acquirer(
        cache_b.clone(),
        Arc::clone(&b_metrics),
        tokio_util::sync::CancellationToken::new(),
    );

    // Drive FIND_VALUE demand to cross the trigger threshold (the gates run in
    // `decide`), then fire the acquisition exactly as the DHT handler does.
    let hash_bytes = *hash.as_bytes();
    assert_eq!(
        engine.on_find_value(&hash_bytes, 0),
        decdn_node::prefetch::PrefetchOutcome::BelowThreshold
    );
    assert_eq!(
        engine.on_find_value(&hash_bytes, 1),
        decdn_node::prefetch::PrefetchOutcome::Decided(
            decdn_node::prefetch::decision::PrefetchDecision::Acquire
        )
    );
    engine.try_acquire(hash_bytes);

    // Await the background acquisition (bounded): wait for BOTH the blob to
    // land in B's cache AND the prefetch tag to be recorded. The acquirer writes
    // the tag strictly *after* the blob is resident (see
    // `crates/node/src/prefetch/acquirer.rs`), so polling only on `has(hash)`
    // races the tag. The cheap in-memory tag check is ordered first to
    // short-circuit the async cache probe until the tag is recorded.
    // The poll budget (35s) deliberately exceeds the acquirer's configured
    // deadline (`acquisition_timeout_secs`, default 30s) so a slow-but-correct
    // pull on loaded CI is not declared a failure before the acquirer itself
    // would give up. The happy path breaks in well under 1s, so this budget is
    // only ever spent on a genuine hang.
    let mut ready = false;
    for _ in 0..350 {
        if engine.acquired().contains(&hash_bytes, 2) && cache_b.has(hash).await? {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !ready {
        // Report both legs so a CI timeout names the stuck step without a rerun.
        let cached = cache_b.has(hash).await?;
        let tagged = engine.acquired().contains(&hash_bytes, 2);
        anyhow::bail!(
            "prefetch acquisition did not cache and tag the blob in time \
             (cached={cached}, tagged={tagged})"
        );
    }

    // The paid pull recorded non-zero spend into the prefetch ledger + metric.
    let spend = counter_value(&b_metrics, "prefetch_spend_usdc_total")?;
    anyhow::ensure!(spend > 0, "expected non-zero prefetch spend, got {spend}");
    anyhow::ensure!(
        counter_value(&b_metrics, "prefetch_acquire_succeeded_total")? == 1,
        "expected one successful prefetch acquisition"
    );

    ep_b.close().await;
    ep_a.close().await;
    task_a.abort();
    Ok(())
}

/// Build a cache pre-seeded with every payload in `payloads`: a one-shard
/// filesystem origin holds them, the cache pulls each into its local store, then
/// the origin is dropped. A multi-blob sibling of `support::cache_with_blob`,
/// used by #900's negative control where node A must hold a second, *non*-
/// prefetched blob.
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
    // A reactively serves its OWN origin on a miss (#1116) — the chained pull B
    // triggers. `pull_authorized` still gates it on B proving channel ownership.
    handler_a.attach_local_populate(Duration::from_secs(20));
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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

/// #900: the demand-quality numerator must be fed through the *live* serve loop.
/// After node B speculatively prefetch-acquires a blob (tagging it as prefetch
/// content), a real paid `cdn/client/v1` pull of that blob *from B* drives
/// `PrefetchEngine::note_served_if_prefetched` from inside `collect_voucher`,
/// moving the `served / acquired` demand-quality ratio off zero. The two halves
/// (tag-on-acquire and the `note_served_credits_only_prefetched_hashes` unit
/// test for tag→credit) are tested in isolation elsewhere; this pins the wiring
/// through the real `ClientHandler` serve path.
///
/// A negative control then serves an *untagged* blob (one B reactively pulled on
/// a cache miss, never prefetch-acquired) through the same live handler and
/// asserts the ratio is unchanged — pinning the `acquired.contains` gate, not
/// just that *some* credit fires.
#[tokio::test(flavor = "multi_thread")]
// test setup; failures should panic loudly. `similar_names`: the hash/hash2 and
// c_buyer/c2_buyer pairs are the positive vs negative-control fixtures.
#[allow(clippy::expect_used, clippy::too_many_lines, clippy::similar_names)]
async fn prefetch_acquired_blob_credits_served_through_serve_loop() -> Result<()> {
    let payload = vec![0xCDu8; PAYLOAD_LEN];
    let hash = Hash::new(&payload);
    let hash_bytes = *hash.as_bytes();
    let total_bytes = u64::try_from(PAYLOAD_LEN).unwrap_or(u64::MAX);

    // A second, same-sized blob node A also holds. It is never prefetch-acquired;
    // the negative control serves it through B's live handler to prove an
    // untagged blob does not move the demand-quality ratio. Same length as the
    // first so the hand-rolled probe responder's fixed `total_bytes` fits both.
    let payload2 = vec![0xEEu8; PAYLOAD_LEN];
    let hash2 = Hash::new(&payload2);
    let hash2_bytes = *hash2.as_bytes();

    // --- Node A: holds both blobs; serves probe + client (the prefetch upstream).
    let (cache_a, _tmp_a) = cache_with_blobs(&[payload.as_slice(), payload2.as_slice()]).await?;
    anyhow::ensure!(
        cache_a.has(hash).await? && cache_a.has(hash2).await?,
        "node A must hold both fixture blobs"
    );
    let a_sk = fresh_key();
    let a_id = a_sk.public();
    let a_eth = Arc::new(PrivateKeySigner::random());
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let ab_channel_id = B256::repeat_byte(0xC1);
    let store_a = Arc::new(MemoryChannelStateStore::new());
    store_a.record(&ChannelState::new(
        ab_channel_id,
        b_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;
    let metrics_a = Arc::new(Metrics::new());
    let limiter_a = permissive_limiter(&metrics_a);
    let domains = HandlerDomains {
        slash: slash_domain(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    };
    let handler_a = build_handler_full(
        a_id,
        &a_eth,
        &metrics_a,
        limiter_a,
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

    // --- Node B: prefetch-enabled NodeOrigin AND a live ClientHandler server. ---
    // Its endpoint carries `ALPN_CLIENT` so the same NodeId can both dial A for
    // the speculative pull and later accept client C's paid serve request.
    let b_sk = fresh_key();
    let b_id = b_sk.public();
    let (ep_b, addr_b) = local_endpoint(b_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    // Prime B's iroh address cache with A's address (NodeId-only dialing).
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        hash_bytes,
        1,
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) =
        one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth.address());

    let prefetch_cfg = decdn_common::config::ResolvedPrefetch {
        enabled: true,
        require_authorized_origin: true,
        budget_usdc_per_hour: 1_000_000,
        find_value_threshold: 2,
        ..Default::default()
    };
    let authorized_dir: Arc<dyn OriginDirectory> =
        Arc::new(ConfigOriginDirectory::new(HashMap::from([(
            DhtHash::from_bytes(hash_bytes),
            vec![DhtNodeId::from_bytes(*a_id.as_bytes())],
        )])));
    let engine = Arc::new(decdn_node::prefetch::PrefetchEngine::new(
        prefetch_cfg,
        authorized_dir,
    ));
    let observer = Arc::new(decdn_node::prefetch::PrefetchAcquisitionObserver::new(
        Arc::clone(&engine),
        Arc::clone(&b_metrics),
    )) as Arc<dyn decdn_node::node_origin::AcquisitionObserver>;

    let recorded: Arc<Mutex<Vec<ProgressEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let buyer = Arc::new(StubOpener {
        channel_id: ab_channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        signer: Arc::clone(&b_buyer),
        voucher_domain: voucher_dom(),
        recorded: Arc::clone(&recorded),
    }) as Arc<dyn ChannelOpener>;
    let origin = NodeOrigin::new();
    origin.provision(NodeOriginDeps {
        endpoint: ep_b.clone(),
        routing_table: Arc::new(Mutex::new(RoutingTable::new(DhtNodeId::from_bytes(
            *b_id.as_bytes(),
        )))),
        staker_set: Arc::new(ConfigStakerSet::empty()) as Arc<dyn StakerSet>,
        origin_directory: Arc::new(ConfigOriginDirectory::new(HashMap::from([
            (DhtHash::from_bytes(hash_bytes), providers.clone()),
            // blob2 is discoverable on A too, for the negative control's
            // reactive (non-prefetch) pull-through.
            (DhtHash::from_bytes(hash2_bytes), providers),
        ]))) as Arc<dyn OriginDirectory>,
        addr_resolver: Arc::new(StaticNodeAddressDirectory::new(addr_map))
            as Arc<dyn NodeAddressResolver>,
        buyer,
        self_id: DhtNodeId::from_bytes(*b_id.as_bytes()),
        slash_domain: slash_domain(),
        bind_domain: binding_dom(),
        local_rep: Arc::clone(&local_rep),
        obs_buffer: Arc::clone(&obs_buffer),
        network_rep: Arc::new(
            NetworkReputation::new(NetworkReputationConfig::default())
                .expect("network reputation config"),
        ),
        rep_cfg: NetworkReputationConfig::default(),
        negative_cache: NegativeProbeCache::new(),
        metrics: Arc::clone(&b_metrics),
        region_accountant: empty_region_accountant(),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout: Duration::from_secs(20),
            stall_timeout: Duration::from_secs(20),
            max_blob_size_bytes: 0,
            enable_0rtt: false,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            lookup: decdn_node::dht::LookupConfig::default(),
        },
        acquisition_observer: Some(observer),
    });

    let cache_dir_b = tempfile::tempdir()?;
    let cache_b = CacheEngine::open(
        cache_dir_b.path(),
        vec![Arc::new(origin.clone()) as Arc<dyn Origin>],
        16,
    )
    .await?;
    anyhow::ensure!(!cache_b.has(hash).await?, "B should start without the blob");

    engine.provision_acquirer(
        cache_b.clone(),
        Arc::clone(&b_metrics),
        tokio_util::sync::CancellationToken::new(),
    );

    // --- Prefetch-acquire the blob into B (tags it as prefetch content). -------
    assert_eq!(
        engine.on_find_value(&hash_bytes, 0),
        decdn_node::prefetch::PrefetchOutcome::BelowThreshold
    );
    assert_eq!(
        engine.on_find_value(&hash_bytes, 1),
        decdn_node::prefetch::PrefetchOutcome::Decided(
            decdn_node::prefetch::decision::PrefetchDecision::Acquire
        )
    );
    engine.try_acquire(hash_bytes);

    // Wait for BOTH the blob to land in the cache AND the acquisition's
    // prefetch tag to be recorded in `engine.acquired()`. The acquirer writes
    // the tag strictly *after* the blob is resident (`populate` then a `has`
    // re-check, see `crates/node/src/prefetch/acquirer.rs`), so polling only on
    // `has(hash)` races the tag (PR #1019 CI flake). The cheap in-memory tag
    // check is ordered first so it short-circuits the async cache probe until
    // the tag is recorded.
    let mut ready = false;
    for _ in 0..350 {
        if engine.acquired().contains(&hash_bytes, 2) && cache_b.has(hash).await? {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !ready {
        // Report both legs so a CI timeout names the stuck step without a rerun.
        let cached = cache_b.has(hash).await?;
        let tagged = engine.acquired().contains(&hash_bytes, 2);
        anyhow::bail!(
            "prefetch acquisition did not cache and tag the blob in time \
             (cached={cached}, tagged={tagged})"
        );
    }

    // Baseline: the acquisition seeded the demand-quality *denominator*, but
    // nothing has been served yet, so the ratio sits at zero. (`now = 0`: the
    // ledger's real-time records are never pruned by a tiny query clock —
    // `saturating_sub` floors the age at 0, always inside the window. This
    // relies on the positive default `demand_quality_window_secs`; a zero window
    // would make `0 >= 0` prune everything.)
    let initial_ratio = engine.policy().demand_quality_ratio(0);
    anyhow::ensure!(
        initial_ratio.abs() < f64::EPSILON,
        "expected a zero served/acquired baseline, got {initial_ratio}"
    );

    // --- Node B as a serving node: a real paid client pull of the tagged blob. -
    // Two client channels are registered up front: C1 pulls the tagged blob
    // (positive), C2 pulls the untagged blob2 (negative control).
    let b_eth = Arc::new(PrivateKeySigner::random());
    let c_buyer = Arc::new(PrivateKeySigner::random());
    let c2_buyer = Arc::new(PrivateKeySigner::random());
    let bc_channel_id = B256::repeat_byte(0xC2);
    let bc_channel_id2 = B256::repeat_byte(0xC3);
    let store_b = Arc::new(MemoryChannelStateStore::new());
    store_b.record(&ChannelState::new(
        bc_channel_id,
        c_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;
    store_b.record(&ChannelState::new(
        bc_channel_id2,
        c2_buyer.address(),
        TOKEN,
        U256::from(DEPOSIT_MICRO_USDC),
    ))?;
    let limiter_b = permissive_limiter(&b_metrics);
    let handler_b = build_handler_full(
        b_id,
        &b_eth,
        &b_metrics,
        limiter_b,
        cache_b.clone(),
        store_b as Arc<dyn ChannelStateStore>,
        RATE,
        &domains,
        0,
        16,
    )?;
    handler_b.attach_prefetch_engine(Arc::clone(&engine));
    let task_b = spawn_server(ep_b.clone(), handler_b);

    let (ep_c, _addr_c) = local_endpoint(fresh_key(), vec![]).await?;
    let c_ctx = ChannelContext {
        channel_id: bc_channel_id,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        client_signer: Arc::clone(&c_buyer),
        voucher_domain: voucher_dom(),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    };
    let got = stream_fetch(
        &ep_c,
        EndpointAddr::new(b_id).with_ip_addr(addr_b),
        &c_ctx,
        &slash_domain(),
        b_eth.address(),
        hash_bytes,
        0,
        0x00c0_ffee,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "delivered bytes mismatch"
    );

    // The live serve loop credited the served bytes: the ratio moved off zero.
    let final_ratio = engine.policy().demand_quality_ratio(0);
    anyhow::ensure!(
        final_ratio > initial_ratio,
        "serving a prefetch-acquired blob must raise the demand-quality ratio \
         ({initial_ratio} -> {final_ratio})"
    );
    // The whole blob was acquired (denominator) and the whole blob was served
    // back through the live handler (numerator: summed over both voucher
    // intervals), so served/acquired is exactly 1.0. Asserting the precise value
    // — not merely `> 0` — catches a partial credit, e.g. crediting only the
    // first 1 MiB voucher interval would leave the ratio at ~0.67 and pass a
    // `> 0` check.
    anyhow::ensure!(
        (final_ratio - 1.0).abs() < 1e-9,
        "expected served/acquired == 1.0 after a fully-served prefetch blob, got {final_ratio}"
    );

    // --- Negative control: an *untagged* blob served through the same live
    // handler must NOT move the ratio. B reactively pulls blob2 from A via a
    // plain cache-miss `populate` (the demand path, which never tags the blob as
    // prefetch content), so serving it credits nothing.
    cache_b.populate(hash2).await?;
    anyhow::ensure!(
        cache_b.has(hash2).await?,
        "B should hold the reactively-pulled blob2"
    );
    anyhow::ensure!(
        !engine.acquired().contains(&hash2_bytes, 2),
        "a reactively-pulled blob must not be tagged as prefetch content"
    );

    let c2_ctx = ChannelContext {
        channel_id: bc_channel_id2,
        token: TOKEN,
        deposit: U256::from(DEPOSIT_MICRO_USDC),
        client_signer: Arc::clone(&c2_buyer),
        voucher_domain: voucher_dom(),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    };
    let got2 = stream_fetch(
        &ep_c,
        EndpointAddr::new(b_id).with_ip_addr(addr_b),
        &c2_ctx,
        &slash_domain(),
        b_eth.address(),
        hash2_bytes,
        0,
        0x00c0_fffe,
        Duration::from_secs(20),
    )
    .await?;
    anyhow::ensure!(
        got2.as_ref() == payload2.as_slice(),
        "negative-control delivered bytes mismatch"
    );

    // The untagged serve fired `note_served_if_prefetched` (the live wiring) but
    // the `acquired.contains` gate rejected the credit, so the ratio holds at 1.0.
    let ratio_after_untagged = engine.policy().demand_quality_ratio(0);
    anyhow::ensure!(
        (ratio_after_untagged - 1.0).abs() < 1e-9,
        "serving an untagged blob must leave served/acquired unchanged at 1.0, \
         got {ratio_after_untagged}"
    );

    ep_c.close().await;
    ep_b.close().await;
    ep_a.close().await;
    task_b.abort();
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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

    // Outbound capture (T3): exactly one positive observation about A, and A's
    // local score rose above the 0.5 neutral after a clean delivery.
    let drained = obs_buffer.drain();
    anyhow::ensure!(
        drained.len() == 1,
        "expected one observation, got {}",
        drained.len()
    );
    let (peer, m) = drained
        .first()
        .ok_or_else(|| anyhow::anyhow!("no observation drained"))?;
    anyhow::ensure!(*peer == a_id, "observation recorded about the wrong peer");
    anyhow::ensure!(
        m.data_correct == Some(true) && m.uptime_observed == Some(true),
        "expected positive delivery metrics, got {m:?}"
    );
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
/// before streaming. Because B opens its tee sink *before* it dials upstream, the
/// `received` signal proves B's coalescing owner-pull is in flight and B's cache
/// is still empty — so a test can open a second same-hash request against B while
/// the gate is held and deterministically drive it into the `TeeOpen::InFlight`
/// coalescing branch (#895/#305: one upstream pull, no double spend). Modelled on
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
        anyhow::bail!("gated upstream: expected a StreamRequest");
    };
    // The upstream request landed (B's owner tee is in flight, cache still empty);
    // hold here until the test has opened the coalescing second request.
    received.notify_one();
    release.notified().await;
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
    Ok(())
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
            reason: VoucherRejectReason::StaleNonce,
        }))?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("write voucher rejection: {e}"))?;
    let _ = send.finish();
    conn.closed().await;
    Ok(())
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
                    let _ = serve_then_reject_voucher(conn, &eth, &dom, &served, rate).await;
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
            false,
            None,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
        &b_metrics,
        &region_accountant,
        vec![s_dht, a_dht],
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
        0,
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
    // records NO reputation observation, local or gossiped (#857). That it was
    // attempted at all is proven by the `node_pull_timeout` counter below.
    let drained = obs_buffer.drain();
    anyhow::ensure!(
        drained.len() == 1,
        "expected one observation (honest fallback only; the timed-out staller is exonerated), got {}",
        drained.len()
    );
    anyhow::ensure!(
        !drained.iter().any(|(p, _)| *p == s_id),
        "the timed-out staller must NOT be gossiped about (#857)"
    );
    // The other half of the fix: no LOCAL EWMA hit either. `record_outcome` writes
    // the gossip buffer and the local score together, so a future split that
    // re-introduced a local-only timeout penalty would pass the obs-buffer check
    // above but fail here. The staller stays at the neutral cold-start 0.5.
    anyhow::ensure!(
        (local_rep.score(s_id) - 0.5).abs() < f64::EPSILON,
        "the timed-out staller's local score must stay neutral, got {}",
        local_rep.score(s_id)
    );
    let (_, honest_m) = drained
        .iter()
        .find(|(p, _)| *p == a_id)
        .ok_or_else(|| anyhow::anyhow!("no observation about the honest provider"))?;
    anyhow::ensure!(
        honest_m.data_correct == Some(true) && honest_m.uptime_observed == Some(true),
        "honest provider should score a clean delivery, got {honest_m:?}"
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
#[tokio::test]
// multi-node fixture setup, like its siblings above; the two wedged nodes are
// deliberately named in parallel (`w_*` / `w2_*`) so the pair reads as a pair.
#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn a_wedged_channel_open_does_not_starve_the_candidate_loop() -> Result<()> {
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
            false,
            None,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
    let drained = obs_buffer.drain();
    for (wedged_id, label) in [(w_id, "W1"), (w2_id, "W2")] {
        anyhow::ensure!(
            !drained.iter().any(|(p, _)| *p == wedged_id),
            "{label}: a provider whose channel open wedged must not be gossiped about (#1143)"
        );
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
            false,
            None,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
        &b_metrics,
        &region_accountant,
        vec![s1_dht, s2_dht, a_dht],
        addr_map,
        per_candidate,
        Duration::from_secs(20),
        0,
    );

    // The open loop must abandon BOTH stallers on their own budgets and open A.
    let opened = tokio::time::timeout(Duration::from_secs(12), origin.open_progressive_pull(hash))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "open_progressive_pull never returned: a stalled candidate consumed the whole \
                 outer budget, so the honest fallback was never opened"
            )
        })?;
    let (header, _pull) = opened
        .ok_or_else(|| anyhow::anyhow!("expected an open against the honest fallback candidate"))?;
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
    let drained = obs_buffer.drain();
    for (label, s_id) in [("S1", s1_id), ("S2", s2_id)] {
        anyhow::ensure!(
            !drained.iter().any(|(p, _)| *p == s_id),
            "the timed-out staller {label} must NOT be gossiped about (#857)"
        );
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
    let obs_buffer = Arc::new(ObservationBuffer::new());
    let b_metrics = Arc::new(Metrics::new());
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &obs_buffer,
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
        obs_buffer.drain().is_empty(),
        "no reputation on a no-provider miss"
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;
    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
    let b_metrics = Arc::new(Metrics::new());
    // Provider discovered, but addr_map is EMPTY → unresolvable.
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        B256::repeat_byte(0xA1),
        &Arc::new(PrivateKeySigner::random()),
        &local_rep,
        &obs_buffer,
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
        obs_buffer.drain().is_empty(),
        "an unresolvable provider must NOT be scored (not its fault)"
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
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
    let drained = obs_buffer.drain();
    let (peer, m) = drained
        .first()
        .ok_or_else(|| anyhow::anyhow!("expected an Unreachable observation"))?;
    anyhow::ensure!(*peer == a_id, "observation about wrong peer");
    anyhow::ensure!(
        m.uptime_observed == Some(false) && m.data_correct.is_none(),
        "probe failure must score Unreachable, got {m:?}"
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
    let drained = obs_buffer.drain();
    let (peer, m) = drained
        .first()
        .ok_or_else(|| anyhow::anyhow!("expected a Corruption observation"))?;
    anyhow::ensure!(*peer == a_id, "observation about wrong peer");
    anyhow::ensure!(
        m.data_correct == Some(false) && m.uptime_observed == Some(true),
        "wrong bytes must score Corruption (reachable, incorrect), got {m:?}"
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
    );

    let (ep_b, _) = local_endpoint(fresh_key(), vec![]).await?;
    let b_id = fresh_key().public();
    let _ = probe_once(
        &ep_b,
        EndpointAddr::new(a_id).with_ip_addr(addr_a),
        *hash.as_bytes(),
        1,
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
        obs_buffer.drain().is_empty(),
        "a buyer-side voucher rejection must not emit a reputation observation (#857)"
    );
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
    let drained = obs_buffer.drain();
    let (peer, m) = drained
        .first()
        .ok_or_else(|| anyhow::anyhow!("expected an Unreachable observation"))?;
    anyhow::ensure!(*peer == a_id, "observation about wrong peer");
    anyhow::ensure!(
        m.uptime_observed == Some(false) && m.data_correct.is_none(),
        "a real transport failure must score Unreachable, got {m:?}"
    );
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
    let chunk = encode_message(&ClientMessage::ChunkData(ChunkData::new(vec![
        0x5Au8;
        CHUNK_SIZE
    ])?))?;
    for _ in 0..prefix_chunks {
        write_frame(&mut send, &chunk)
            .await
            .map_err(|e| anyhow::anyhow!("write chunk: {e}"))?;
    }
    // Go quiet. `send` is held (never finished, never reset), so the buyer sees no
    // EOF and no error — only silence. Nothing but the inactivity deadline ends
    // this.
    conn.closed().await;
    Ok(())
}

/// Spawn a provider that answers probes truthfully, opens the client stream
/// honestly, and then goes silent mid-delivery (see [`serve_then_go_silent`]).
fn spawn_a_mid_stream_silent_server(
    ep: iroh::Endpoint,
    a_eth: Arc<PrivateKeySigner>,
    slash: Eip712Domain,
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
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ =
                        serve_then_go_silent(conn, &eth, &dom, total_bytes, rate, prefix_chunks)
                            .await;
                });
            }
        }
    })
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
    let chunk = encode_message(&ClientMessage::ChunkData(ChunkData::new(vec![
        0x77u8;
        CHUNK_SIZE
    ])?))?;
    // Two real frames, well under the 1 MiB voucher interval, so no voucher round trip
    // intrudes and the loop is unambiguously mid-delivery when the error lands.
    for _ in 0..2 {
        write_frame(&mut send, &chunk)
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
            if conn.alpn() == ALPN_PROBE {
                tokio::spawn(async move {
                    let _ = answer_probe(conn, &eth, &dom, rate, total_bytes).await;
                });
            } else {
                tokio::spawn(async move {
                    let _ =
                        serve_then_error_mid_stream(conn, &eth, &dom, total_bytes, rate, err).await;
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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
    let (_header, mut pull) =
        tokio::time::timeout(Duration::from_secs(10), origin.open_progressive_pull(hash))
            .await
            .map_err(|_| anyhow::anyhow!("the progressive open never returned"))?
            .ok_or_else(|| {
                anyhow::anyhow!("expected a clean open against the empty-chunk upstream")
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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

    let drained = obs_buffer.drain();
    let (peer, m) = drained
        .first()
        .ok_or_else(|| anyhow::anyhow!("a mid-stream stall must be gossiped (#1134)"))?;
    anyhow::ensure!(*peer == a_id, "observation recorded about the wrong peer");
    anyhow::ensure!(
        m.uptime_observed == Some(false) && m.data_correct.is_none(),
        "a mid-stream stall must score Unreachable, got {m:?}"
    );
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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
    anyhow::ensure!(
        obs_buffer.drain().is_empty(),
        "an honest mid-stream refusal must not be gossiped as an observation"
    );
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
    let task_s1 = spawn_a_mid_stream_silent_server(
        ep_s1.clone(),
        Arc::clone(&s1_eth),
        slash_domain(),
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
            false,
            None,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;

    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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
/// ~20 s of transfer was simply unfetchable, and the background warm that should
/// have rescued it was capped by the same budget. No test caught that, because
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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
            false,
            None,
            Duration::from_secs(10),
        )
        .await?;
    }

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
    }) as Arc<dyn ChannelOpener>;
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
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
    let drained = obs_buffer.drain();
    anyhow::ensure!(
        !drained.iter().any(|(p, _)| *p == n_id),
        "a node that honestly refused with NotFound must not be gossiped about (#1144)"
    );
    anyhow::ensure!(
        (local_rep.score(n_id) - 0.5).abs() < f64::EPSILON,
        "an honest NotFound must leave the refusing node's local score neutral, got {}",
        local_rep.score(n_id)
    );
    // The one observation is the honest delivery from A — the refusal did not
    // suppress scoring in general, it is only NotFound that is exonerated.
    anyhow::ensure!(
        drained.len() == 1 && drained.iter().any(|(p, _)| *p == a_id),
        "expected exactly one observation (A's clean delivery), got {drained:?}"
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
    let drained = obs_buffer.drain();
    let (peer, m) = drained.first().ok_or_else(|| {
        anyhow::anyhow!("a self-reported InternalError must be gossiped, not exonerated (#1144)")
    })?;
    anyhow::ensure!(*peer == a_id, "observation recorded about the wrong peer");
    anyhow::ensure!(
        m.uptime_observed == Some(false) && m.data_correct.is_none(),
        "an InternalError refusal must score Unreachable, got {m:?}"
    );
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
        obs_buffer.drain().is_empty(),
        "a buyer-side ceiling rejection must not emit a reputation observation"
    );
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
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
        &obs_buffer,
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
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Arc<Metrics>,
    Arc<LocalReputation>,
)> {
    build_node_b_with_leaves(
        a_id,
        a_addr,
        a_eth_addr,
        hash,
        ab_channel_id,
        b_buyer,
        &[(leaf_channel_id, leaf_eth_addr, leaf_deposit)],
        max_blob_size_bytes,
        64,
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
#[allow(clippy::too_many_arguments)]
async fn build_node_b_with_leaves(
    a_id: iroh::PublicKey,
    a_addr: std::net::SocketAddr,
    a_eth_addr: Address,
    hash: Hash,
    ab_channel_id: B256,
    b_buyer: &Arc<PrivateKeySigner>,
    leaves: &[(B256, Address, U256)],
    max_blob_size_bytes: u64,
    engine_max_blob_mb: u64,
) -> Result<(
    Arc<decdn_node::handlers::client::ClientHandler>,
    EndpointAddr,
    iroh::Endpoint,
    Arc<Mutex<Vec<ProgressEntry>>>,
    decdn_cache::CacheEngine,
    Arc<Metrics>,
    Arc<LocalReputation>,
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
        false,
        None,
        Duration::from_secs(10),
    )
    .await?;

    let local_rep = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
    let obs_buffer = Arc::new(ObservationBuffer::new());
    let b_metrics = Arc::new(Metrics::new());
    let (providers, addr_map) = one_provider(DhtNodeId::from_bytes(*a_id.as_bytes()), a_eth_addr);
    let (origin, recorded) = provisioned_origin(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        ab_channel_id,
        b_buyer,
        &local_rep,
        &obs_buffer,
        &b_metrics,
        providers,
        addr_map,
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
    for (leaf_channel_id, leaf_eth_addr, leaf_deposit) in leaves {
        store_b.record(&ChannelState::new(
            *leaf_channel_id,
            *leaf_eth_addr,
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
    let handler_b = build_handler_full(
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
    )?;
    // Window-paced pull-through: the deadline accommodates the full
    // discover→probe→pull, and the window is the default ~1 MiB (one interval).
    handler_b.attach_pull_through(Duration::from_secs(20));
    handler_b.attach_window_pull_through(
        Arc::new(origin),
        decdn_cache::Bytes::new(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES),
    );

    let target = EndpointAddr::new(b_id).with_ip_addr(addr_b);
    Ok((
        handler_b,
        target,
        ep_b,
        recorded,
        cache_handle,
        b_metrics,
        local_rep,
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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
    // B's buyer channel to A advanced to the full blob (one persisted watermark
    // covering all bytes, nonce 2). Under ADR 038 the node-to-node payment meters
    // WIRE bytes (the bao stream: content + interleaved proof), so the watermark
    // covers the bao-encoded size with the amount rounded up per the rate — not
    // the content size.
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
    let (handler_b, b_target, ep_b, recorded, cache_b, _b_metrics, _local_rep) = build_node_b(
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
/// request owns the tee sink and pulls from A; the second hits `TeeOpen::InFlight`
/// and waits on the coalesced fill (`await_coalesced_fill`) rather than opening a
/// second upstream pull — which would double-spend real USDC on the B↔A channel.
/// The cache-level coalescing primitive is unit-tested (`engine.rs`
/// `tee_sink_coalesces_concurrent_fills`); this pins the handler-side consequence
/// at the layer that actually spends.
///
/// Determinism: a gated upstream A parks after receiving B's (single) upstream
/// request. Because B opens its tee sink before dialing upstream, the gate signal
/// proves the owner pull is in flight and B's cache is still empty, so the second
/// leaf — launched while the gate is held — is expected to coalesce. The
/// no-double-spend
/// assertions hold for every interleaving regardless.
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) =
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
                    U256::from(DEPOSIT_MICRO_USDC),
                ),
                (
                    leaf2_channel_id,
                    leaf2_eth.address(),
                    U256::from(DEPOSIT_MICRO_USDC),
                ),
            ],
            0,
            64,
        )
        .await?;
    let task_b = spawn_server_concurrent(ep_b.clone(), handler_b);

    // Leaf 1: the owner pull. Spawn it, then wait for A to confirm B's single
    // upstream request landed (tee in flight, cache empty).
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
    // and leaf 1's tee owns the in-flight fill, so leaf 2 hits `TeeOpen::InFlight`.
    // The brief pause lets leaf 2 reach that branch before we release A; the
    // no-double-spend assertions below hold regardless of interleaving.
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

    // Release A: the single upstream pull completes, the tee promotes the blob,
    // and leaf 2's coalesced wait resolves and serves from cache.
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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

    // The fused window path must never have run: no pause, no tee finalize, no
    // upstream verify — and crucially B must have made NO upstream pull (empty
    // progress log) and cached NOTHING. A regression that dropped the offset-0
    // gate would trip at least the upstream pull (non-empty log) and likely the
    // tee.
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_window_paused_total")? == 0,
        "fused window pause must not fire for a resumed (offset>0) request"
    );
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_tee_finalize_failed_total")? == 0,
        "fused tee must not run for a resumed (offset>0) request"
    );
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_upstream_verify_failed_total")? == 0,
        "fused upstream verify must not run for a resumed (offset>0) request"
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
    let (handler_b, b_target, ep_b, recorded, cache_b, _b_metrics, _local_rep) = build_node_b(
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

    // The headline assertion: B's persisted upstream spend is bounded to ~one
    // window (it forwarded one interval, collected the voucher, then hit the
    // dropped connection on the next chunk). It must be far below the whole blob.
    let log = progress_log(&recorded)?;
    let upstream_bytes: u64 = log.last().map_or(0, |(_, _, bytes, _)| {
        u64::try_from(*bytes).unwrap_or(u64::MAX)
    });
    let one_window = decdn_common::config::DEFAULT_PULL_AHEAD_BYTES;
    anyhow::ensure!(
        upstream_bytes <= one_window + 2 * (CHUNK_SIZE as u64),
        "B's upstream spend ({upstream_bytes}) must be bounded to ~one window ({one_window}), \
         not the whole {total_bytes}-byte blob"
    );
    anyhow::ensure!(
        upstream_bytes < total_bytes,
        "B must not have pulled the whole blob ({upstream_bytes} vs {total_bytes})"
    );
    // B did not complete the fill, so it must NOT have cached the blob.
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "an abandoned fill must not promote the blob into B's cache"
    );

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
    // max_blob_size_bytes = 64 MiB → ceiling = min_payment(64 MiB, RATE) = 640
    // µUSDC; the leaf's deposit of 1 µUSDC cannot cover it. The blob itself is
    // well under 64 MiB, so this is the deposit guard firing, not the size gate.
    let max_blob_size_bytes = 64 * 1024 * 1024;
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
        a_id,
        a_addr,
        a_eth.address(),
        hash,
        ab_channel_id,
        &b_buyer,
        leaf_channel_id,
        leaf_eth.address(),
        U256::from(1u64),
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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
    handler_b.attach_leech_governor(Arc::new(LeechGovernor::new(
        LeechCaps::new_unchecked(LeechCapsConfig {
            max_unrecouped_leech_bytes: Bytes::new(0),
            initial_allowance_bytes: Bytes::new(CHUNK_SIZE as u64),
            share_ratio_percent: Percent::new(0),
        }),
        Arc::clone(&b_metrics),
    )));
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
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, local_rep) = build_node_b(
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
    // Mutually exclusive with the local-fault arms: a lying upstream must NOT
    // read as a failing local disk (#915 review).
    assert_counter(&b_metrics, "node_pull_through_local_tee_failed_total", 0)?;
    assert_counter(&b_metrics, "node_pull_through_tee_finalize_failed_total", 0)?;
    // The tee verdict reached the scorer: A recorded a `Corruption` observation,
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
    // with plenty of wire still to come — kills the tee's verifying decoder while
    // the forward loop is still writing, so the failure surfaces as a `tee.write`
    // error, NOT at finalization. This is the dominant real-world corruption
    // shape (the finalize arm only fires when the whole remaining wire fits in
    // the tee channel slack, i.e. tiny blobs like the sibling test above).
    // Pre-fix this arm was misclassified as a LOCAL store fault:
    // `local_tee_failed` (the operator's failing-disk alarm) fired, no reputation
    // outcome was recorded, and the corrupt upstream kept its score. It must
    // instead: abandon the pull early (bounded spend), not promote, fire
    // `upstream_verify_failed` (and NOT `local_tee_failed`), and score A
    // `Corruption`.
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
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, local_rep) = build_node_b(
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
    // The whole point of the fix: mid-stream corruption is the UPSTREAM's fault,
    // not a local disk fault.
    assert_counter(&b_metrics, "node_pull_through_local_tee_failed_total", 0)?;
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

#[tokio::test(flavor = "multi_thread")]
async fn window_pull_through_tee_finalize_failure_serves_but_does_not_cache() -> Result<()> {
    // #896: the sibling of `window_pull_through_lying_upstream_is_not_cached`. Here
    // the upstream is HONEST (the promised wire byte count arrives and the bytes
    // verify, so `pull.finish(..)` is Ok and the tee verdict is not a hash
    // mismatch), but B's cache-engine store rejects the promote at
    // `tee.finish()` for a LOCAL reason. The bytes were already forwarded and
    // paid, so delivery MUST still complete (the leaf gets every byte plus
    // `StreamEnd`); only the warm-cache benefit is forfeited. This is the
    // alertable "served but not cached" path: the
    // `node_pull_through_tee_finalize_failed` counter fires while the
    // `upstream_verify_failed` counter does not. (A tee HashMismatch is NOT this
    // arm — it routes to `upstream_verify_failed` with no `StreamEnd`, covered by
    // the lying-upstream and mid-stream-corruption siblings, #915.)
    //
    // Deterministic store-fault injection without a mock: B's engine cap
    // (`engine_max_blob_mb = 1` → 1 MiB) sits exactly one byte below the blob
    // (1 MiB + 1). The cap is enforced by `count_and_cap_stream` over the
    // DECODED plaintext the tee's bao decoder emits, so a breach mid-fill would
    // generally kill the import task and surface as a later in-loop `tee.write`
    // failure instead. Sizing the blob to exactly `cap + 1` CONTENT bytes lands
    // the breach on the FINAL decoded leaf: every `tee.write` of the wire has
    // already succeeded and there is no further write, so the overrun surfaces
    // only at `tee.finish()` — the branch under test. The handler cap stays `0`
    // (unlimited) so the serve is never rejected up front.
    let payload = vec![0xC9u8; 1024 * 1024 + 1];
    let hash = Hash::new(&payload);
    let total_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);

    let ab_channel_id = B256::repeat_byte(0xC9);
    let b_buyer = Arc::new(PrivateKeySigner::random());
    let (a_id, a_addr, a_eth, ep_a, task_a) =
        spawn_node_a(&payload, ab_channel_id, b_buyer.address()).await?;

    let leaf_eth = Arc::new(PrivateKeySigner::random());
    let leaf_channel_id = B256::repeat_byte(0x9C);
    let (handler_b, b_target, ep_b, _recorded, cache_b, b_metrics, _local_rep) =
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
                U256::from(DEPOSIT_MICRO_USDC),
            )],
            0, // handler cap: unlimited, so the serve proceeds and `pull.finish()` is Ok
            1, // engine cap: 1 MiB = total - 1, so `tee.finish()` rejects the promote
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

    // Delivery still succeeded despite the failed promote: the leaf saw `StreamEnd`
    // and received every (hash-verified) byte.
    anyhow::ensure!(
        outcome.completed,
        "delivery must still complete when only the cache promote fails"
    );
    anyhow::ensure!(outcome.hash_ok, "leaf received bytes failed the hash check");
    anyhow::ensure!(
        outcome.received == total_bytes,
        "leaf received {} of {total_bytes} bytes",
        outcome.received
    );
    // ...and the leaf actually PAID for those bytes (this is "served", not free):
    // a 1 MiB+ blob crosses at least one voucher interval, so ≥1 ack must land.
    anyhow::ensure!(
        outcome.acks > 0,
        "the served bytes must have been paid for (no voucher ack observed)"
    );
    // The warm-cache benefit was forfeited: B is NOT a holder for this blob.
    anyhow::ensure!(
        !cache_b.has(hash).await?,
        "B must not promote a blob whose tee finalize failed"
    );
    // The alertable "served but not cached" metric fired exactly once...
    assert_counter(&b_metrics, "node_pull_through_tee_finalize_failed_total", 1)?;
    // ...and we hit the tee-finalize branch, NOT the upstream-verify branch (the
    // upstream was honest) nor the in-loop `tee.write` failure branch.
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_upstream_verify_failed_total")? == 0,
        "honest upstream must not trip the upstream-verify branch"
    );
    anyhow::ensure!(
        counter_value(&b_metrics, "node_pull_through_local_tee_failed_total")? == 0,
        "a final-chunk cap breach must surface at finish, not as an in-loop tee.write failure"
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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
    anyhow::ensure!(
        upstream_bytes <= one_window + 2 * (CHUNK_SIZE as u64),
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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
    let gov = Arc::new(LeechGovernor::new(
        // `new_unchecked`: a tiny global budget below the opening window, so the
        // global circuit breaker binds on the first admission (the scenario under
        // test). `LeechCaps::new` rejects this pairing by design.
        LeechCaps::new_unchecked(LeechCapsConfig {
            max_unrecouped_leech_bytes: Bytes::new(CHUNK_SIZE as u64),
            initial_allowance_bytes: Bytes::new(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES),
            share_ratio_percent: Percent::new(100),
        }),
        Arc::clone(&b_metrics),
    ));
    // Pre-exhaust the global budget through an unrelated peer.
    gov.record_pulled(
        &[0xEEu8; 32],
        decdn_common::config::DEFAULT_PULL_AHEAD_BYTES,
    );
    handler_b.attach_leech_governor(gov);
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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
    let (handler_b, b_target, ep_b, recorded, cache_b, b_metrics, _local_rep) = build_node_b(
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
    // No opening allowance, no share-ratio growth, global budget off: the peer is
    // immediately over its (zero) ceiling at the first admission poll. These caps
    // satisfy `LeechCaps::new` (a `0` global budget disables the window≤budget
    // cross-check), so the validated constructor is used here.
    let gov = Arc::new(LeechGovernor::new(
        LeechCaps::new(LeechCapsConfig {
            max_unrecouped_leech_bytes: Bytes::new(0),
            initial_allowance_bytes: Bytes::new(0),
            share_ratio_percent: Percent::new(0),
        })
        .map_err(|e| anyhow::anyhow!("invalid caps: {e}"))?,
        Arc::clone(&b_metrics),
    ));
    handler_b.attach_leech_governor(gov);
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
