//! End-to-end test for origin-tier range pull-through on `cdn/client/v1`
//! (#823, ADR 037 §Origin-tier pull-through; closes the #990 coverage gap).
//!
//! A `cdn/client/v1` byte-range request (`StreamRequest` with `byte_offset` /
//! `byte_len`) against a **cold** cache must fetch only the requested span
//! from origin — not the whole blob — bao-verify it against the content
//! address, and assemble the correct bytes back to the paying client. The
//! engine machinery (`pull_through_range` / `export_range`) and the origin
//! adapters are unit-tested in `decdn-cache`; this test exercises the full
//! protocol path through the [`ClientHandler`].
//!
//! Two scenarios:
//! 1. The origin publishes the sibling `{H}.obao4` outboard → the handler
//!    range-pulls (HEAD for the size, outboard GET, one `206` ranged data GET),
//!    serves the span, and the blob stays **partial** (never a whole-blob GET).
//! 2. The origin does NOT publish the outboard → the range pull declines and
//!    the handler falls back to the buffered whole-blob pull, still serving the
//!    correct range bytes ("fallback is always correct", ADR 037).

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::BytesMut;
use decdn_cache::range_pull::{IROH_BLOCK_SIZE, align_range};
use decdn_cache::{CacheEngine, Hash, HttpOrigin, Origin};
use decdn_incentive::{
    ChannelState, ChannelStateStore, EPHEMERAL_BINDING_NONCE, MemoryChannelStateStore, Voucher,
    bind_node_id_domain, binding_signing_hash, signed_to_wire_voucher, slash_judge_domain,
    voucher_domain,
};
use decdn_node::handlers::client::ClientHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::client::{ClientBinding, ClientMessage, StreamRequest, StreamRequestExt};
use decdn_protocol::{
    ALPN_CLIENT, DEFAULT_VOUCHER_INTERVAL_MB, MB_BYTES, encode_stream_request, write_frame,
};
use iroh::EndpointAddr;
use iroh::endpoint::SendStream;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod support;
use support::{
    HandlerDomains, build_handler_full_configured, fresh_key, local_endpoint, permissive_limiter,
    read_client_msg, spawn_server, write_client_msg,
};

const CHAIN_ID: u64 = 421_614;
const TOKEN: Address = Address::repeat_byte(0x22);
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

/// A distinctive 200 KiB payload (so a 16 KiB-aligned sub-range is a strict
/// interior slice) plus its pre-order bao outboard and content hash.
fn blob_with_outboard() -> (Vec<u8>, Vec<u8>, Hash) {
    let blob: Vec<u8> = (0..200 * 1024u32)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    (blob, ob.data, hash)
}

/// Register a single channel owned by `client` and build a `ClientHandler` over
/// a cache whose only origin is `origin_uri`. Returns the handler, a cache
/// clone (so the test can inspect `has` after delivery), and the `Metrics`
/// handle (so a test can assert per-reason reject counters).
async fn handler_over_http_origin(
    origin_uri: &str,
    channel_id: B256,
    client: Address,
    server_eth: &Arc<PrivateKeySigner>,
    server_id: iroh::PublicKey,
    pull_through: Option<Duration>,
) -> anyhow::Result<(
    Arc<ClientHandler>,
    CacheEngine,
    Arc<Metrics>,
    tempfile::TempDir,
)> {
    let store = Arc::new(MemoryChannelStateStore::new());
    store.record(&ChannelState::new(
        channel_id,
        client,
        client,
        TOKEN,
        U256::from(10_000_000u64),
    ))?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(origin_uri)?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn ChannelStateStore> = store;
    let handler = build_handler_full_configured(
        server_id,
        server_eth,
        &metrics,
        limiter,
        cache.clone(),
        store_dyn,
        RATE_PER_MB,
        &domains(),
        0,
        16,
        |deps| deps.pull_through = pull_through,
    )?;
    Ok((handler, cache, metrics, cache_dir))
}

/// Read a `decdn_<name>` counter's value from the encoded metrics registry.
/// `name` omits the `decdn_` prefix (e.g. `serve_stream_rejected_..._total`).
fn counter_value(metrics: &Arc<Metrics>, name: &str) -> anyhow::Result<u64> {
    let text = metrics
        .encode()
        .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
    let prefix = format!("decdn_{name} ");
    Ok(text
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0))
}

/// A manual `cdn/client/v1` client that requests the bounded range
/// `[byte_offset, byte_offset + byte_len)` with an ownership binding (so the
/// server's `pull_authorized` gate passes), pays the vouchers the server
/// collects, and returns the assembled range bytes. Modeled on
/// `node_origin_pull::leaf_paced_pull`, but the closing-voucher trigger keys on
/// the requested `byte_len` — the server advertises the *whole-blob*
/// `total_bytes`, yet only the range is delivered.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn ranged_paid_pull(
    client_ep: &iroh::Endpoint,
    target: EndpointAddr,
    client_node_id: B256,
    client_eth: &Arc<PrivateKeySigner>,
    channel_id: B256,
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
        voucher_interval_mb: None,
        binding: Some(ClientBinding {
            ethereum_address: client_eth.address().into(),
            binding_signature,
        }),
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id.into(),
        byte_offset,
        byte_len,
        timestamp_us: 0x9001,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;

    let resp = match read_client_msg(&mut recv).await? {
        ClientMessage::StreamResponse(r) => r,
        other => anyhow::bail!("expected StreamResponse, got {other:?}"),
    };
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp.error);
    // The advertised size is the *whole* blob; the range delivers `byte_len`, or
    // the whole tail when `byte_len == 0`. Reject an impossible advertised total
    // rather than masking a server bug: a signed `total_bytes` before the offset
    // or shorter than the requested range end is surfaced here.
    if byte_len == 0 {
        resp.body
            .total_bytes
            .checked_sub(byte_offset)
            .ok_or_else(|| anyhow::anyhow!("response total_bytes is before byte_offset"))?;
    } else {
        let end = byte_offset
            .checked_add(byte_len)
            .ok_or_else(|| anyhow::anyhow!("requested range overflows"))?;
        anyhow::ensure!(
            end <= resp.body.total_bytes,
            "response total_bytes is smaller than the requested range end"
        );
    }
    // ADR 038: the wire carries the bao verified-stream (content + interleaved
    // proof) for the group-aligned superset of the request, so the paid/closing
    // boundary is the bao-encoded WIRE size, not the requested content length.
    let aligned =
        decdn_cache::range_pull::align_range(byte_offset, byte_len, resp.body.total_bytes)
            .map_err(|e| anyhow::anyhow!("align range: {e}"))?;
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(resp.body.total_bytes, aligned.chunk_ranges());
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    let mut nonce: u64 = 0;
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
                    nonce += 1;
                    let amount = U256::from(cumulative)
                        .saturating_mul(U256::from(rate))
                        .div_ceil(U256::from(MB_BYTES));
                    let signed = Voucher {
                        channel_id,
                        amount,
                        nonce: U256::from(nonce),
                        bytes_delivered: U256::from(cumulative),
                        token: TOKEN,
                    }
                    .sign(client_eth.as_ref(), &voucher_dom())
                    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
                    write_client_msg(
                        &mut send,
                        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
                    )
                    .await?;
                    match read_client_msg(&mut recv).await? {
                        ClientMessage::VoucherAck => {}
                        ClientMessage::StreamError(e) => {
                            anyhow::bail!("voucher rejected: {e:?}")
                        }
                        other => anyhow::bail!("expected VoucherAck, got {other:?}"),
                    }
                    unvouchered = 0;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("stream error mid-delivery: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }
    conn.close(0u32.into(), b"done");
    // The wire is the header-less bao verified-stream for the aligned superset;
    // decode + verify it against the root and trim to the requested span (ADR
    // 038), so callers compare against the exact range content as before.
    decode_bao_range(hash, resp.body.total_bytes, byte_offset, byte_len, &buf)
}

/// Decode the header-less bao verified-stream `wire` for `[byte_offset,
/// byte_offset+byte_len)` (`byte_len == 0` ⇒ to end), verifying every chunk
/// group against `hash`, and trim the group-aligned superset back to the exact
/// requested span. Mirrors the production receiver (`client-pull`).
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

    let aligned = decdn_cache::range_pull::align_range(byte_offset, byte_len, total_bytes)
        .map_err(|e| anyhow::anyhow!("align range: {e}"))?;
    let tree = BaoTree::new(total_bytes, decdn_cache::range_pull::IROH_BLOCK_SIZE);
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

/// Count `received_requests` matching a predicate, failing loudly if wiremock
/// recording was disabled (which would make the assertion silently pass).
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
async fn cold_range_request_pulls_only_the_range_from_origin() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    // The requested sub-range and the chunk-group-aligned span the engine will
    // actually fetch (computed with the same helper the engine uses).
    let (req_off, req_len) = (16 * 1024u64, 32 * 1024u64);
    let aligned = align_range(req_off, req_len, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    // (1) HEAD → canonical blob size (no body); the origin size probe.
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    // (2) sibling outboard GET.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // (3) ranged data GET → 206 with exactly the aligned span. NOTE: there is
    // deliberately NO whole-blob (un-ranged) GET mounted, so any attempt to
    // pull the whole blob would 404 and fail — the range path must be used.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .mount(&server)
        .await;

    let channel_id = B256::repeat_byte(0x42);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        channel_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

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
        channel_id,
        hash,
        req_off,
        req_len,
        RATE_PER_MB,
    )
    .await?;

    // The delivered bytes are exactly the requested sub-range of the blob.
    let want = blob
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "delivered range mismatch: got {} bytes, want {}",
        got.len(),
        want.len()
    );

    // The origin served the range, NOT the whole blob: exactly one ranged GET
    // and zero un-ranged GETs on the data object.
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets == 1,
        "expected one ranged data GET, got {ranged_gets}"
    );
    anyhow::ensure!(
        wholeblob_gets == 0,
        "the whole blob must never be fetched to serve a range, saw {wholeblob_gets} un-ranged GET(s)"
    );

    // The blob is held only as a partial: a range pull does not promote a
    // complete blob, so `has` stays false (ADR 037 §partial-not-advertised).
    anyhow::ensure!(
        !cache.has(hash).await?,
        "a range pull must leave the blob partial, not a full holder"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn range_request_without_outboard_falls_back_to_whole_blob() -> anyhow::Result<()> {
    let (blob, _outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();
    let (req_off, req_len) = (16 * 1024u64, 32 * 1024u64);

    let server = MockServer::start().await;
    // HEAD answers (the size probe succeeds), but NO `{H}.obao4` is published,
    // so the range pull declines. A plain whole-blob GET IS served, so the
    // buffered fallback can fill the cache.
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(blob.clone()))
        .mount(&server)
        .await;

    let channel_id = B256::repeat_byte(0x43);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    // Enable the buffered whole-blob pull-through the fallback relies on.
    let (handler, cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        channel_id,
        client_eth.address(),
        &server_eth,
        server_id,
        Some(Duration::from_secs(15)),
    )
    .await?;

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
        channel_id,
        hash,
        req_off,
        req_len,
        RATE_PER_MB,
    )
    .await?;

    let want = blob
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "fallback must still deliver the correct range bytes"
    );
    // The fallback fetched the whole blob, so the node is now a full holder.
    anyhow::ensure!(
        cache.has(hash).await?,
        "the whole-blob fallback must complete and cache the blob"
    );
    // Prove the fallback used an *un-ranged* whole-blob GET and never issued a
    // ranged data GET: with no `{H}.obao4` the range pull declines at the
    // outboard probe, so no `Range` request should ever hit the data object.
    let hex = hash.to_hex();
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        ranged_gets == 0,
        "no ranged GET should occur once the outboard probe misses, saw {ranged_gets}"
    );
    anyhow::ensure!(
        wholeblob_gets >= 1,
        "the fallback must fetch the whole blob with an un-ranged GET"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthorized_range_request_triggers_no_origin_fetch() -> anyhow::Result<()> {
    // A ranged cache-miss request WITHOUT an ownership binding must not make the
    // node front any origin egress: `pull_authorized` fails, so the range pull
    // (and the whole-blob fallback) is never initiated. This closes the
    // griefing vector where an unpaid client induces origin HEAD/range traffic.
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    // Mount the full range-pull surface; the assertion is that NONE of it is hit.
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard))
        .mount(&server)
        .await;

    let channel_id = B256::repeat_byte(0x44);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        channel_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Send a bounded ranged request with NO binding in the extension.
    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let ext = StreamRequestExt {
        voucher_interval_mb: None,
        binding: None,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id.into(),
        byte_offset: 16 * 1024,
        byte_len: 32 * 1024,
        timestamp_us: 0x9001,
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;

    // The node refuses (signed `StreamResponse { ok: false }`) — no delivery.
    match read_client_msg(&mut recv).await? {
        ClientMessage::StreamResponse(r) => {
            anyhow::ensure!(!r.body.ok, "unauthorized range request must be refused");
        }
        other => anyhow::bail!("expected a refusing StreamResponse, got {other:?}"),
    }
    conn.close(0u32.into(), b"done");

    // The load-bearing assertion: the origin was never contacted.
    let origin_hits = count_requests(&server, |_| true).await?;
    anyhow::ensure!(
        origin_hits == 0,
        "an unauthorized range request must not front any origin egress, saw {origin_hits} request(s)"
    );
    anyhow::ensure!(
        !cache.has(hash).await?,
        "no blob should have been cached for an unauthorized request"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_to_end_range_pull_serves_tail() -> anyhow::Result<()> {
    // A resume request (`byte_offset > 0`, `byte_len == 0`) is a whole-tail
    // read: `byte_len == 0` means "to end-of-blob" (ADR 005), NOT zero bytes.
    // The handler passes it straight through `pull_through_range` /
    // `export_range`, both of which resolve `0` to the blob end — this proves
    // the tail is fetched and served, end to end.
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let req_off = 16 * 1024u64;
    let aligned = align_range(req_off, 0, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    anyhow::ensure!(a_end == blob_size, "a zero-len tail aligns to the blob end");
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned tail out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span))
        .mount(&server)
        .await;

    let channel_id = B256::repeat_byte(0x46);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        channel_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

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
        channel_id,
        hash,
        req_off,
        0, // byte_len == 0 → resume to end
        RATE_PER_MB,
    )
    .await?;

    let want = blob
        .get(usize::try_from(req_off)?..)
        .ok_or_else(|| anyhow::anyhow!("offset past blob"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "a zero-len resume must deliver the whole tail, got {} of {} bytes",
        got.len(),
        want.len()
    );
    anyhow::ensure!(
        !cache.has(hash).await?,
        "a tail range pull leaves the blob partial"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn out_of_bounds_range_is_rejected_before_delivery() -> anyhow::Result<()> {
    // ADR 005 §Bounded byte ranges: a range whose end exceeds the blob MUST be
    // refused with a `StreamError` — and the refusal must land BEFORE a success
    // response is signed, so the client never accepts an `ok: true` the delivery
    // then aborts. Exercised against a cache hit so the request reaches the size
    // gate directly.
    let (blob, _outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(blob.clone()))
        .mount(&server)
        .await;

    let channel_id = B256::repeat_byte(0x47);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        channel_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;
    // Pre-populate the blob so the ranged request is a cache hit.
    cache.populate(hash).await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let conn = client_ep
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    // `byte_offset` in bounds but `byte_offset + byte_len` past the blob end.
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        channel_id: channel_id.into(),
        byte_offset: 16 * 1024,
        byte_len: blob_size,
        timestamp_us: 0x9001,
    };
    let payload = encode_stream_request(&req, Some(&StreamRequestExt::default()))
        .map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;

    // The refusal must be the very first message and a signed `ok: false` — not
    // a success followed by a mid-stream abort.
    match read_client_msg(&mut recv).await? {
        ClientMessage::StreamResponse(r) => {
            anyhow::ensure!(
                !r.body.ok,
                "an out-of-bounds range must be refused, not served"
            );
        }
        other => anyhow::bail!("expected a refusing StreamResponse, got {other:?}"),
    }
    conn.close(0u32.into(), b"done");

    // Pin the EXACT reject path: the wire `StreamError` collapses to `NotFound`
    // (shared with cache-miss / unknown-channel), so only the per-reason counter
    // proves the range-not-satisfiable gate fired rather than some other refusal.
    anyhow::ensure!(
        counter_value(
            &metrics,
            "serve_stream_rejected_range_not_satisfiable_total"
        )? == 1,
        "the out-of-bounds reject must increment the range-not-satisfiable counter"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}

/// A WHOLE-BLOB cold miss whose own origin publishes the `{H}.obao4` outboard is
/// served through the Flow A own-origin two-leg path (`serve_via_backend_origin`,
/// FA.3a): dispatch confirms serviceability (origin size + published outboard),
/// signs `ok:true`, and runs the local pull leg (fill the cache from origin)
/// beside the serve leg (stream the filling cache to the paying client). This
/// asserts the dispatch selection reaches `serve_via_backend_origin` — the
/// `decdn_local_outboard_serves_total` tier counter fires once — and that the
/// client receives a coherent, hash-verifying whole-blob delivery. There is no
/// upstream, channel, or payment on the ingest side; the client still pays the
/// downstream vouchers exactly as any paid delivery.
#[tokio::test(flavor = "multi_thread")]
async fn whole_blob_own_origin_miss_serves_via_backend_origin() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    // The whole-blob request aligns to [0, blob_size); compute the ranged span the
    // local pull leg will fetch with the same helper the engine uses.
    let aligned = align_range(0, 0, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    // (1) HEAD → canonical blob size: the dispatch serviceability size probe.
    Mock::given(method("HEAD"))
        .and(path(format!("/{hex}")))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Content-Length", blob_size.to_string()),
        )
        .mount(&server)
        .await;
    // (2) sibling outboard GET: the dispatch serviceability outboard probe AND the
    //     pull leg's range-encode outboard fetch (may be hit more than once).
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // (3) ranged data GET → 206 with the whole aligned span (the single full-miss
    //     gap the local pull leg draws).
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .mount(&server)
        .await;

    let channel_id = B256::repeat_byte(0x51);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        channel_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Whole blob: byte_offset == 0 && byte_len == 0 — the exact gate dispatch uses
    // to route to the own-origin two-leg path.
    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        channel_id,
        hash,
        0,
        0,
        RATE_PER_MB,
    )
    .await?;

    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "whole-blob own-origin delivery mismatch: got {} bytes, want {}",
        got.len(),
        blob.len()
    );

    // The own-origin serve-miss tier fired exactly once — proof dispatch selected
    // `serve_via_backend_origin` rather than a fallback tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "the own-origin two-leg serve tier must fire once"
    );

    client_ep.close().await;
    server_ep.close().await;
    server_task.await?;
    Ok(())
}
