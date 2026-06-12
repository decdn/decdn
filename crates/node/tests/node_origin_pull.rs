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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;
use async_trait::async_trait;
use decdn_cache::Hash;
use decdn_cache::origin::{Origin, OriginFetch};
use decdn_incentive::{
    ChannelState, ChannelStateStore, MemoryChannelStateStore, ProbeSlashData, StreamSlashData,
    bind_node_id_domain, slash_judge_domain, voucher_domain,
};
use decdn_node::buyer_channel::ChannelOpener;
use decdn_node::client_requester::ChannelContext;
use decdn_node::dht::negative_cache::Hash as DhtHash;
use decdn_node::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use decdn_node::dht::{
    ConfigOriginDirectory, ConfigStakerSet, NegativeProbeCache, NodeAddressResolver,
    OriginDirectory, StakerSet, StaticNodeAddressDirectory,
};
use decdn_node::metrics::Metrics;
use decdn_node::node_origin::{NodeOrigin, NodeOriginConfig, NodeOriginDeps};
use decdn_node::probe_client::probe_once;
use decdn_protocol::client::{
    ChunkData, ClientMessage, StreamError, StreamResponse, StreamResponseBody, VoucherRejectReason,
};
use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};
use decdn_protocol::{
    ALPN_CLIENT, ALPN_PROBE, CHUNK_SIZE, ProbeMessage, decode_message, encode_message, read_frame,
    write_frame,
};
use decdn_reputation::{
    LocalReputation, LocalReputationConfig, NetworkReputation, NetworkReputationConfig,
    ObservationBuffer,
};
use iroh::EndpointAddr;
use iroh::endpoint::Connection;

mod support;
use support::{
    HandlerDomains, build_handler_full, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter,
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
        })
    }

    fn record_progress(
        &self,
        provider_addr: Address,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<()> {
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

#[async_trait]
impl ChannelOpener for FailingRecordOpener {
    async fn open_or_reuse_channel(
        &self,
        _provider_addr: Address,
        _deposit_hint: U256,
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
        })
    }

    fn record_progress(
        &self,
        _provider_addr: Address,
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
        ep_b, b_dht, hash, buyer, local_rep, obs_buffer, metrics, providers, addr_map,
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
        providers,
        addr_map,
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
        providers,
        addr_map,
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
    providers: Vec<DhtNodeId>,
    addr_map: HashMap<DhtNodeId, Address>,
    pull_timeout: Duration,
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
        local_rep: Arc::clone(local_rep),
        obs_buffer: Arc::clone(obs_buffer),
        network_rep: Arc::new(
            NetworkReputation::new(NetworkReputationConfig::default())
                .expect("network reputation config"),
        ),
        rep_cfg: NetworkReputationConfig::default(),
        negative_cache: NegativeProbeCache::new(),
        metrics: Arc::clone(metrics),
        config: NodeOriginConfig {
            probe_fanout: 5,
            pull_timeout,
            max_blob_size_bytes,
            enable_0rtt: false,
            deposit_hint: U256::from(DEPOSIT_MICRO_USDC),
            lookup: decdn_node::dht::LookupConfig::default(),
        },
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
    // 1.5 MiB at RATE 10/MiB over a 1-MiB interval ⇒ two vouchers: the closing
    // one carries nonce 2, the full 1,572,864 bytes, and the cumulative amount 15
    // (10 for the first MiB + 5 for the trailing half).
    anyhow::ensure!(
        progress_log(&recorded)?
            == vec![(
                a_eth.address(),
                U256::from(2),
                U256::from(total_bytes),
                U256::from(15)
            )],
        "expected one persisted progress entry with the final voucher totals, got {:?}",
        progress_log(&recorded)?
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
            &encode_message(&ClientMessage::ChunkData(ChunkData {
                bytes: chunk.to_vec(),
            }))?,
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
            &encode_message(&ClientMessage::ChunkData(ChunkData {
                bytes: chunk.to_vec(),
            }))?,
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
    if let ClientMessage::Voucher(_) = voucher_msg {
        write_frame(
            &mut send,
            &encode_message(&ClientMessage::StreamError(StreamError::VoucherRejected {
                reason: VoucherRejectReason::StaleNonce,
            }))?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write voucher rejection: {e}"))?;
    }
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
    let origin = build_origin_with_timeout(
        &ep_b,
        DhtNodeId::from_bytes(*b_id.as_bytes()),
        hash,
        buyer,
        &local_rep,
        &obs_buffer,
        &b_metrics,
        vec![s_dht, a_dht],
        addr_map,
        // Short per-candidate budget so the stall is abandoned quickly. With the
        // pre-#859 wiring an equal outer deadline would have cancelled the whole
        // fetch here; at the `NodeOrigin` level there is no outer wrapper, so this
        // exercises the per-candidate fallthrough the fix preserves.
        Duration::from_secs(1),
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

    ep_b.close().await;
    ep_a.close().await;
    ep_s.close().await;
    task_a.await?;
    task_s.await?;
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
    // than resetting: nonce 2 → 4, cumulative bytes 1.5 MiB → 3 MiB, amount 15 → 30.
    let log = progress_log(&recorded)?;
    anyhow::ensure!(
        log == vec![
            (
                a_eth.address(),
                U256::from(2),
                U256::from(total_bytes),
                U256::from(15)
            ),
            (
                a_eth.address(),
                U256::from(4),
                U256::from(2 * total_bytes),
                U256::from(30),
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
