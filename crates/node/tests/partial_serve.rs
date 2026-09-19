//! End-to-end test for the partial-serve gate (#1506): a node holding only
//! *part* of a blob must serve a `cdn/client/v1` request that falls entirely
//! within its cached ranges directly from cache, without attempting any
//! fill/pull-through — the counterpart to the will-serve coverage a partial
//! holder now advertises (probe/DHT, #1506 tasks 2-3). Before this gate,
//! `serve_audit` is `Complete`-only, so a partial holder always fell into the
//! miss path and terminated `NotFound` even for bytes it already has.
//!
//! Two scenarios:
//! 1. A request entirely within an already-cached chunk group, with NO origin
//!    and NO pull-through configured (nothing else *could* fill it), succeeds
//!    and is paid — proving the new branch serves straight from cache.
//! 2. A request that spans a cached range AND a missing range does not
//!    short-circuit: it takes the own-origin two-leg spine, whose pull leg
//!    fetches only the missing group (asserted via the mock origin actually
//!    being hit), exactly as an ordinary cache-miss range request does.

use std::sync::Arc;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::BytesMut;
use decdn_cache::range_pull::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
use decdn_cache::{CHUNK_GROUP_BYTES, CacheEngine, Hash, HttpOrigin, Origin};
use decdn_incentive::{
    EPHEMERAL_BINDING_NONCE, LaneState, MemoryPoolStateStore, PoolStateStore, Voucher,
    bind_node_id_domain, binding_signing_hash, signed_to_wire_voucher, slash_judge_domain,
    voucher_domain,
};
use decdn_protocol::client::{ClientBinding, ClientMessage, StreamRequest, StreamRequestExt};
use decdn_protocol::{ALPN_CLIENT, CHUNK_BYTES, MB_BYTES, encode_stream_request, write_frame};
use iroh::EndpointAddr;
use iroh::endpoint::SendStream;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod support;
use support::{
    HandlerDomains, build_handler_full, fresh_key, local_endpoint, permissive_limiter,
    read_client_msg, read_stream_response, shutdown, spawn_server, write_client_msg,
};

const CHAIN_ID: u64 = 421_614;
const RATE_PER_MB: u64 = 10;

fn slash_dom() -> Eip712Domain {
    slash_judge_domain(CHAIN_ID, Address::repeat_byte(0x11))
}
fn voucher_dom() -> Eip712Domain {
    voucher_domain(CHAIN_ID, Address::repeat_byte(0x34))
}
fn binding_dom() -> Eip712Domain {
    bind_node_id_domain(CHAIN_ID, Address::repeat_byte(0x99))
}
fn domains() -> HandlerDomains {
    HandlerDomains {
        slash: slash_dom(),
        voucher: voucher_dom(),
        binding: binding_dom(),
    }
}

/// Deterministic pseudo-random payload of `len` bytes (xorshift32) plus its
/// pre-order bao outboard and content hash. Content doesn't matter, only that
/// it hashes and bao-verifies consistently.
fn synth_blob(len: usize) -> (Hash, Vec<u8>, bytes::Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes()[0];
    }
    let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    (hash, plaintext, bytes::Bytes::from(ob.data))
}

/// The header-less interleaved bao for `[off, off + len)` of a blob with the
/// given `plaintext`/`outboard`/`total` size — an `admit_bao`-ready chunk.
fn bao_for(
    hash: Hash,
    plaintext: &[u8],
    outboard: bytes::Bytes,
    off: u64,
    len: u64,
    total: u64,
) -> anyhow::Result<(bao_tree::ChunkRanges, bytes::Bytes)> {
    let aligned = align_range(off, len, total)?;
    let s = usize::try_from(aligned.fetch_start())?;
    let e = usize::try_from(aligned.fetch_end())?;
    let slice = plaintext
        .get(s..e)
        .ok_or_else(|| anyhow::anyhow!("aligned range out of bounds"))?;
    let encoded = encode_verified_range(*hash.as_bytes(), &aligned, slice, outboard)?;
    Ok((aligned.chunk_ranges().clone(), encoded))
}

/// A manual `cdn/client/v1` client that requests `[byte_offset, byte_offset +
/// byte_len)` with an ownership binding, pays the vouchers the server
/// collects, and returns the assembled range bytes.
#[allow(clippy::too_many_arguments)]
async fn ranged_paid_pull(
    client_ep: &iroh::Endpoint,
    target: EndpointAddr,
    client_node_id: B256,
    client_eth: &Arc<PrivateKeySigner>,
    pool_id: B256,
    provider: Address,
    hash: Hash,
    byte_offset: u64,
    byte_len: u64,
    rate: u64,
) -> anyhow::Result<Vec<u8>> {
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let binding_hash =
        binding_signing_hash(client_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    let binding_signature = client_eth
        .sign_hash_sync(&binding_hash)?
        .as_bytes()
        .to_vec();
    let ext = StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: client_eth.address().into(),
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
        timestamp_us: 0x9001,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;

    let (resp, resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error);

    let aligned = align_range(byte_offset, byte_len, resp.body.total_bytes)
        .map_err(|e| anyhow::anyhow!("align range: {e}"))?;
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(resp.body.total_bytes, aligned.chunk_ranges());
    let interval_bytes = CHUNK_BYTES;

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    loop {
        match read_client_msg(&mut recv).await? {
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
                        signer: client_eth.address(),
                        provider,
                        amount,
                        bytes_delivered: U256::from(cumulative),
                        chain_root: B256::ZERO,
                        chunk_price: U256::ZERO,
                    }
                    .sign(client_eth.as_ref(), &voucher_dom())
                    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
                    write_client_msg(
                        &mut send,
                        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)?),
                    )
                    .await?;
                    unvouchered = 0;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("stream error mid-delivery: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }
    conn.close(0u32.into(), b"done");
    decode_bao_range(hash, resp.body.total_bytes, byte_offset, byte_len, &buf)
}

/// Decode the header-less bao verified-stream `wire` for `[byte_offset,
/// byte_offset+byte_len)`, verifying every chunk group against `hash`, and
/// trim the group-aligned superset back to the exact requested span.
fn decode_bao_range(
    hash: Hash,
    total_bytes: u64,
    byte_offset: u64,
    byte_len: u64,
    wire: &[u8],
) -> anyhow::Result<Vec<u8>> {
    use bao_tree::BaoTree;
    use bao_tree::io::BaoContentItem;
    use bao_tree::io::sync::DecodeResponseIter;

    let aligned = align_range(byte_offset, byte_len, total_bytes)
        .map_err(|e| anyhow::anyhow!("align range: {e}"))?;
    let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
    let reader = std::io::Cursor::new(wire);
    let mut plaintext = Vec::new();
    for item in DecodeResponseIter::new(hash.into(), tree, reader, aligned.chunk_ranges().as_ref())
    {
        match item.map_err(|e| anyhow::anyhow!("bao decode: {e}"))? {
            BaoContentItem::Leaf(leaf) => plaintext.extend_from_slice(&leaf.data),
            BaoContentItem::Parent(_) => {}
        }
    }
    let lead = usize::try_from(byte_offset.saturating_sub(aligned.fetch_start()))?;
    let want = if byte_len == 0 {
        plaintext.len().saturating_sub(lead)
    } else {
        usize::try_from(byte_len)?
    };
    let end = lead.saturating_add(want);
    let slice = plaintext
        .get(lead..end)
        .ok_or_else(|| anyhow::anyhow!("decoded range shorter than requested span"))?;
    Ok(slice.to_vec())
}

async fn write_frame_to(send: &mut SendStream, payload: &[u8]) -> anyhow::Result<()> {
    write_frame(send, payload)
        .await
        .map_err(|e| anyhow::anyhow!("write frame: {e}"))
}

async fn count_requests(
    server: &MockServer,
    pred: impl Fn(&wiremock::Request) -> bool,
) -> anyhow::Result<usize> {
    let reqs = server
        .received_requests()
        .await
        .ok_or_else(|| anyhow::anyhow!("wiremock request recording disabled"))?;
    Ok(reqs.iter().filter(|r| pred(r)).count())
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_serve_within_cached_range_serves_without_any_fill() -> anyhow::Result<()> {
    let group = CHUNK_GROUP_BYTES;
    let total = 5 * group;
    let (hash, plaintext, outboard) = synth_blob(usize::try_from(total)?);

    let cache_dir = tempfile::tempdir()?;
    // No origins: nothing can fill this cache reactively. If the partial-serve
    // gate doesn't fire, this request has no other way to succeed.
    let cache = CacheEngine::open(cache_dir.path(), vec![], 16).await?;

    // Admit chunk group 0 (the requested range lives entirely inside it) and
    // the trailing group (so iroh-blobs learns the blob's total size — it
    // only reports one for a `Partial` blob once the final chunk is present).
    // The middle groups stay missing: the blob is genuinely partial.
    let (g0_ranges, g0_bao) = bao_for(hash, &plaintext, outboard.clone(), 0, group, total)?;
    cache.admit_bao(hash, g0_ranges, g0_bao).await?;
    let (tail_ranges, tail_bao) = bao_for(hash, &plaintext, outboard, total - group, group, total)?;
    cache.admit_bao(hash, tail_ranges, tail_bao).await?;

    let pool_id = B256::repeat_byte(0x42);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    // `build_handler_full` (no `configure` closure) defaults `pull_through`,
    // `local_populate`, and `pull_through_origin` to `None` — no active fill
    // of any kind is wired up.
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id,
        client_eth.address(),
        provider,
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let metrics = Arc::new(decdn_node::metrics::Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let handler = build_handler_full(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        &domains(),
        16,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // A small request entirely within chunk group 0.
    let (req_off, req_len) = (256u64, 1024u64);
    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        req_off,
        req_len,
        RATE_PER_MB,
    )
    .await?;

    let want = plaintext
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "delivered range mismatch: got {} bytes, want {}",
        got.len(),
        want.len()
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn partial_serve_mixed_range_pulls_only_the_missing_group() -> anyhow::Result<()> {
    let group = CHUNK_GROUP_BYTES;
    let total = 5 * group;
    let (hash, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let hex = hash.to_hex();

    let cache_dir = tempfile::tempdir()?;
    let server = MockServer::start().await;
    // HEAD → canonical blob size.
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", total.to_string()))
        .mount(&server)
        .await;
    // sibling outboard GET.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.to_vec()))
        .mount(&server)
        .await;
    // The requested span is group 0 + group 1. Group 0 is already cached, group 1
    // is not, so the pull leg's `missing_ranges` is exactly group 1 — and that is
    // the ONLY ranged GET the origin may see. Mount a 206 for group 1 alone; a
    // request for the whole span (a re-fetch of the held group) 404s and fails.
    let (req_off, req_len) = (0u64, 2 * group);
    let gap = align_range(group, group, total)?;
    let gap_bytes = plaintext
        .get(usize::try_from(gap.fetch_start())?..usize::try_from(gap.fetch_end())?)
        .ok_or_else(|| anyhow::anyhow!("gap span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={}-{}", gap.fetch_start(), gap.fetch_end() - 1);
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(wiremock::matchers::header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(gap_bytes))
        .mount(&server)
        .await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;
    // Pre-admit ONLY group 0 locally — the node genuinely holds part of the
    // blob, but the requested span reaches past it into an absent group.
    let (g0_ranges, g0_bao) = bao_for(hash, &plaintext, outboard.clone(), 0, group, total)?;
    cache.admit_bao(hash, g0_ranges, g0_bao).await?;

    let pool_id = B256::repeat_byte(0x43);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id,
        client_eth.address(),
        provider,
        U256::from(10_000_000u64),
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let metrics = Arc::new(decdn_node::metrics::Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let handler = build_handler_full(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        &domains(),
        16,
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        req_off,
        req_len,
        RATE_PER_MB,
    )
    .await?;

    let want = plaintext
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "delivered range mismatch: got {} bytes, want {}",
        got.len(),
        want.len()
    );

    // The mixed request neither short-circuited as a cache hit nor re-fetched the
    // held group: exactly one ranged GET, and it is the gap (group 1) alone.
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets == 1,
        "expected exactly one ranged GET for the missing group, got {ranged_gets}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}
