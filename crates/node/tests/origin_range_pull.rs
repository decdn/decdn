//! End-to-end tests for the own-origin two-leg serve-miss on `cdn/client/v1`
//! (ADR 037 §Origin-tier pull-through).
//!
//! A `cdn/client/v1` request of any shape (`StreamRequest` with `byte_offset` /
//! `byte_len`) against a **cold** cache must sign its response first, fetch only
//! the requested span's missing chunk groups from origin — not the whole blob —
//! bao-verify each group against the content address as it lands, and stream
//! the correct bytes back to the paying client. The engine machinery
//! (`origin_range_wire` / `export_bao_range_stream`) and the origin adapters
//! are unit-tested in `decdn-cache`; these tests exercise the full protocol path
//! through the [`ClientHandler`].
//!
//! Two shapes of origin:
//! 1. The origin publishes `{H}`, `{H}.obao4` and `{H}.sizeproof` → the handler
//!    answers from the memoised size proof, then streams after admission
//!    (outboard GETs, `206` ranged data GETs per draw), serves the span, and a
//!    partial request leaves the blob **partial** (never a whole-blob GET).
//! 2. One of those objects is missing or broken, or the origin ignores `Range`
//!    → an origin fault: the stream ends before any `ChunkData`, and the node
//!    commits nothing and never reads the whole blob.

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::BytesMut;
use decdn_cache::range_pull::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
use decdn_cache::{CacheEngine, Hash, HttpOrigin, Origin};
use decdn_incentive::{
    EPHEMERAL_BINDING_NONCE, LaneState, MemoryPoolStateStore, PoolStateStore, Voucher,
    bind_node_id_domain, binding_signing_hash, signed_to_wire_voucher, slash_judge_domain,
    voucher_domain,
};
use decdn_node::handlers::client::ClientHandler;
use decdn_node::handlers::timestamp_skew::now_us;
use decdn_node::metrics::Metrics;
use decdn_protocol::client::{ClientBinding, ClientMessage, StreamRequest, StreamRequestExt};
use decdn_protocol::{ALPN_CLIENT, CHUNK_BYTES, MB_BYTES, encode_stream_request, write_frame};
use iroh::EndpointAddr;
use iroh::endpoint::SendStream;
use wiremock::matchers::{header, header_exists, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

mod support;
use support::{
    HandlerDomains, build_handler_full_configured, fresh_key, local_endpoint, permissive_limiter,
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

/// Register a single lane spending `pool_id` (capability signer `client`, paying
/// the node operator `server_eth`) and build a `ClientHandler` over a cache whose
/// only origin is `origin_uri`. Returns the handler, a cache clone (so the test
/// can inspect `has` after delivery), and the `Metrics` handle (so a test can
/// assert per-reason reject counters).
async fn handler_over_http_origin(
    origin_uri: &str,
    pool_id: B256,
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
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id,
        client,               // capability signer
        server_eth.address(), // provider (the serving node operator)
        U256::from(10_000_000u64),
        u64::MAX, // expiry: far future, never expires in-test
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(origin_uri)?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let handler = build_handler_full_configured(
        server_id,
        server_eth,
        &metrics,
        limiter,
        cache.clone(),
        store_dyn,
        RATE_PER_MB,
        &domains(),
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
/// collects ahead of delivery, and returns the assembled range bytes. Modeled on
/// `node_origin_pull::leaf_paced_pull`, but the last voucher keys on the
/// requested `byte_len` — the server proves the *whole-blob* size, yet only the
/// range is delivered.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
        pull_through: true,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset,
        byte_len,
        timestamp_us: now_us(),
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;

    let (resp, resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "delivery refused: {:?}", resp_ext.error());
    let total_bytes = resp_ext
        .size_proof()
        .map(decdn_protocol::SizeProof::size)
        .ok_or_else(|| anyhow::anyhow!("an ok response carries a size proof"))?;
    // The advertised size is the *whole* blob; the range delivers `byte_len`, or
    // the whole tail when `byte_len == 0`. `byte_offset` must be inside the
    // advertised total: a proven size at or before the offset is surfaced here
    // as a server bug. An end past the advertised total is not a bug: it clamps
    // and serves up to the blob's end, so it is NOT checked here.
    // `align_range_clamped` below is the single source of truth for the served
    // (and billed) span.
    if byte_len == 0 {
        total_bytes
            .checked_sub(byte_offset)
            .ok_or_else(|| anyhow::anyhow!("response size is before byte_offset"))?;
    } else {
        anyhow::ensure!(
            byte_offset < total_bytes,
            "response size is at or before the requested offset"
        );
    }
    // ADR 038: the wire carries the bao verified-stream (content + interleaved
    // proof) for the group-aligned superset of the request, so the paid/closing
    // boundary is the bao-encoded WIRE size, not the requested content length.
    // `align_range_clamped`, because `byte_len` may run past `total_bytes`: the
    // server serves and bills that clamped to the blob's end rather than
    // refusing it.
    let aligned = decdn_cache::range_pull::align_range_clamped(byte_offset, byte_len, total_bytes)
        .map_err(|e| anyhow::anyhow!("align range: {e}"))?;
    let expected_wire =
        decdn_cache::range_pull::bao_encoded_size(total_bytes, aligned.chunk_ranges());
    let interval_bytes = CHUNK_BYTES;

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    // The node sends only against the stream's credit (ADR 003 § Credit Window),
    // so the client pays one interval ahead and tops the credit up each time
    // delivery reaches it. The last voucher pays exactly `expected_wire`.
    // Cumulative amounts over the lane's lifetime — vouchers are monotone in
    // `amount` (there is no nonce). Each voucher assigns its new bytes to this
    // stream (`stream_bytes`). Acceptance is implicit (ADR 005): a rejection
    // arrives as a mid-stream `StreamError`.
    let pay = |paid: u64, share: u64| -> anyhow::Result<ClientMessage> {
        let amount = U256::from(paid)
            .saturating_mul(U256::from(rate))
            .div_ceil(U256::from(MB_BYTES));
        let signed = Voucher {
            pool_id,
            signer: client_eth.address(),
            provider,
            amount,
            bytes_delivered: U256::from(paid),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(client_eth.as_ref(), &voucher_dom())
        .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
        let mut wire = signed_to_wire_voucher(&signed)?;
        wire.stream_bytes = share;
        Ok(ClientMessage::Voucher(wire))
    };
    let mut paid = interval_bytes.min(expected_wire);
    if paid > 0 {
        write_client_msg(&mut send, &pay(paid, paid)?).await?;
    }
    loop {
        match read_client_msg(&mut recv).await? {
            ClientMessage::ChunkData(chunk) => {
                buf.extend_from_slice(chunk.bytes());
                let len = u64::try_from(chunk.bytes().len()).unwrap_or(u64::MAX);
                cumulative = cumulative.saturating_add(len);
                if cumulative >= paid && paid < expected_wire {
                    let before = paid;
                    paid = paid.saturating_add(interval_bytes).min(expected_wire);
                    write_client_msg(&mut send, &pay(paid, paid - before)?).await?;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => {
                anyhow::bail!("stream error after {cumulative} bytes: {e:?}")
            }
            other => anyhow::bail!("unexpected message mid-delivery: {other:?}"),
        }
    }
    conn.close(0u32.into(), b"done");
    // The wire is the header-less bao verified-stream for the aligned superset;
    // decode + verify it against the root and trim to the requested span (ADR
    // 038), so callers compare against the exact range content as before.
    decode_bao_range(hash, total_bytes, byte_offset, byte_len, &buf)
}

/// Decode the header-less bao verified-stream `wire` for `[byte_offset,
/// byte_offset+byte_len)` (`byte_len == 0` ⇒ to end, and an end past
/// `total_bytes` clamps to it), verifying every chunk group
/// against `hash`, and trim the group-aligned superset back to the exact
/// served span. Mirrors the production receiver (`decdn-client`).
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

    let aligned = decdn_cache::range_pull::align_range_clamped(byte_offset, byte_len, total_bytes)
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
        // An end past `total_bytes` clamps to the blob's end, same as the server.
        let end = byte_offset.saturating_add(byte_len).min(total_bytes);
        usize::try_from(end.saturating_sub(byte_offset))?
    };
    let end = lead.saturating_add(want);
    let slice = plaintext
        .get(lead..end)
        .ok_or_else(|| anyhow::anyhow!("decoded range shorter than requested span"))?;
    Ok(slice.to_vec())
}

/// The CLI's header-only open ahead of a multi-source fetch: a bound `(0, 0)`
/// request, read the signed `StreamResponse` for `total_bytes`, then close the
/// connection without paying or reading a byte. Returns the advertised total.
async fn throwaway_open(
    client_ep: &iroh::Endpoint,
    target: EndpointAddr,
    client_node_id: B256,
    client_eth: &Arc<PrivateKeySigner>,
    pool_id: B256,
    hash: Hash,
) -> anyhow::Result<u64> {
    let (conn, total) =
        idle_open(client_ep, target, client_node_id, client_eth, pool_id, hash).await?;
    // Exactly what `UpstreamPull::abort` / `Drop` do on the CLI: close the
    // connection. The node's serve leg learns of it at once through
    // `send.stopped()`, and the last-out lease release cancels its fill.
    conn.close(0u32.into(), b"client-abandoned");
    Ok(total)
}

/// A bound `(0, 0)` open that reads the signed `StreamResponse` and then does
/// NOTHING — neither pays nor closes. The node's fill for it parks with no
/// stream credit and its paid frontier at 0 for as long as the connection lives: the
/// stalled-owner shape. Returns the live connection (drop it to end the stream)
/// and the advertised total.
async fn idle_open(
    client_ep: &iroh::Endpoint,
    target: EndpointAddr,
    client_node_id: B256,
    client_eth: &Arc<PrivateKeySigner>,
    pool_id: B256,
    hash: Hash,
) -> anyhow::Result<(iroh::endpoint::Connection, u64)> {
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
        pull_through: true,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: now_us(),
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;
    let (resp, resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "idle open refused: {:?}", resp_ext.error());
    let total_bytes = resp_ext
        .size_proof()
        .map(decdn_protocol::SizeProof::size)
        .ok_or_else(|| anyhow::anyhow!("an ok response carries a size proof"))?;
    // Keep the streams alive with the connection so the node's serve leg stays
    // parked on a client that never pays.
    std::mem::forget(send);
    std::mem::forget(recv);
    Ok((conn, total_bytes))
}

async fn write_frame_to(send: &mut SendStream, payload: &[u8]) -> anyhow::Result<()> {
    write_frame(send, payload)
        .await
        .map_err(|e| anyhow::anyhow!("write frame: {e}"))
}

/// Serve the `{hex}.sizeproof` sibling `decdn origin import` writes, which
/// proves the blob's size before the node states it (ADR 005 § Bounded
/// byte ranges).
async fn mount_size_proof(server: &MockServer, hex: &str, blob: &[u8]) {
    let size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let Ok(eo) = decdn_bao_range::encode_outboard(blob, size) else {
        return;
    };
    Mock::given(method("GET"))
        .and(path(format!(
            "/{hex}{}",
            decdn_bao_range::SIZE_PROOF_SUFFIX
        )))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(eo.size_proof_object()))
        .mount(server)
        .await;
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
#[allow(clippy::too_many_lines)] // one linear integration-test scenario, not real complexity
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
    // (1) the size proof the open answers from.
    mount_size_proof(&server, &hex, &blob).await;
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

    let pool_id = B256::repeat_byte(0x42);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
        pool_id,
        provider,
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

    // The origin served the range, NOT the whole blob: exactly one ranged GET,
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
    // Routing proof: the bounded miss took the own-origin two-leg streaming tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "a bounded cold miss must take the own-origin two-leg tier"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthorized_range_request_triggers_no_origin_fetch() -> anyhow::Result<()> {
    // A ranged cache-miss request WITHOUT an ownership binding must not make the
    // node front any origin egress: with a pool view wired, the node refuses an
    // unbound request before its origin presence read, so no origin read is
    // initiated. This closes the griefing vector where an unpaid client
    // induces origin size-proof/outboard/range traffic.
    let (blob, outboard, hash) = blob_with_outboard();
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    // Mount the full range-pull surface; the assertion is that NONE of it is hit.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x44);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;
    let metrics = Arc::new(Metrics::new());
    let mut status = std::collections::HashMap::new();
    status.insert(
        pool_id,
        decdn_node::pool_view::PoolStatus {
            owner: client_eth.address(),
            remaining: U256::from(10_000_000u64),
            lifecycle: decdn_node::pool_view::Lifecycle::Open,
        },
    );
    let pool_view =
        Arc::new(StatusMapPoolView { status }) as Arc<dyn decdn_node::pool_view::PoolView>;
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        permissive_limiter(&metrics),
        cache.clone(),
        Arc::new(MemoryPoolStateStore::new()),
        RATE_PER_MB,
        &domains(),
        16,
        |deps| support::set_pool_view(deps, pool_view),
    )?;

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
        binding: None,
        capability: None,
        pull_through: true,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset: 16 * 1024,
        byte_len: 32 * 1024,
        timestamp_us: now_us(),
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

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_to_end_range_pull_serves_tail() -> anyhow::Result<()> {
    // A resume request (`byte_offset > 0`, `byte_len == 0`) is a whole-tail
    // read: `byte_len == 0` means "to end-of-blob" (ADR 005), NOT zero bytes.
    // The two-leg spine resolves `0` to the blob end in both legs (`missing_ranges`
    // for the pull, the serve leg's clamp for delivery) — this proves the tail is
    // fetched and served, end to end, and that a resumed miss streams rather than
    // buffering.
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
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x46);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
        pool_id,
        provider,
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
    // Routing proof: the resumed miss took the own-origin two-leg streaming tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "a resumed cold miss must take the own-origin two-leg tier"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn end_past_the_blob_own_origin_cold_miss_serves_clamped_tail() -> anyhow::Result<()> {
    // The twin of `resume_to_end_range_pull_serves_tail`, but with an EXPLICIT
    // `byte_len` (the whole blob's size) rather than the `byte_len == 0` "to
    // end" convention, so the requested end runs well past the blob. The
    // own-origin two-leg tier's pull leg computes its gap via
    // `NodeRangedStore::missing_ranges`, which must clamp the same way the
    // serve tiers clamp: an unclamped `missing_ranges` faults `drive` after
    // the node has already signed `ok: true`.
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let req_off = 16 * 1024u64;
    let aligned = align_range(req_off, 0, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    anyhow::ensure!(a_end == blob_size, "a clamped tail aligns to the blob end");
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned tail out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x49);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
        pool_id,
        provider,
        hash,
        req_off,
        blob_size, // explicit end far past the blob, not `byte_len == 0`
        RATE_PER_MB,
    )
    .await?;

    let want = blob
        .get(usize::try_from(req_off)?..)
        .ok_or_else(|| anyhow::anyhow!("offset past blob"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "an end past the blob must deliver clamped to the blob's end through the \
         own-origin cold-miss tier, got {} of {} bytes",
        got.len(),
        want.len()
    );
    anyhow::ensure!(
        !cache.has(hash).await?,
        "a tail range pull leaves the blob partial"
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "a clamped cold miss must take the own-origin two-leg tier"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn offset_past_the_blob_is_rejected_before_delivery() -> anyhow::Result<()> {
    // ADR 005 §Bounded byte ranges: a range whose START is at or past the blob
    // end MUST be refused with a `StreamError`, and the refusal must land
    // BEFORE a success response is signed, so the
    // client never accepts an `ok: true` the delivery then aborts. Exercised
    // against a cache hit so the request reaches the bounds gate directly.
    let (blob, _outboard, hash) = blob_with_outboard();
    // The fixture handler attests registered payloads.
    support::register_payload(&blob);
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;

    let pool_id = B256::repeat_byte(0x47);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let _provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;
    // Pre-populate the blob so the ranged request is a cache hit.
    cache.insert_blob(blob.clone()).await?;

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
    // `byte_offset` itself is at the blob end: no chunk group to anchor,
    // clamped or not, so this must refuse.
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset: blob_size,
        byte_len: 0,
        timestamp_us: now_us(),
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
                "an offset at or past the blob end must be refused, not served"
            );
        }
        other => anyhow::bail!("expected a refusing StreamResponse, got {other:?}"),
    }
    conn.close(0u32.into(), b"done");

    // Pin the EXACT reject path: the wire `StreamError` collapses to `NotFound`
    // (shared with cache-miss / unknown-lane), so only the per-reason counter
    // proves the range-not-satisfiable gate fired rather than some other refusal.
    anyhow::ensure!(
        counter_value(
            &metrics,
            "serve_stream_rejected_range_not_satisfiable_total"
        )? == 1,
        "the out-of-bounds reject must increment the range-not-satisfiable counter"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn end_past_the_blob_is_served_clamped_not_refused() -> anyhow::Result<()> {
    // ADR 005 §Bounded byte ranges: a request whose start is in bounds but
    // whose end runs past the blob is served up to the blob's end instead of
    // refused: a claimed size is a hint, and this leg reaches the true end,
    // proving it. Exercised against a cache hit so the request reaches the
    // bounds gate directly (the same gate `offset_past_the_blob_is_rejected_...`
    // exercises for a genuinely out-of-bounds start).
    let (blob, _outboard, hash) = blob_with_outboard();
    // The fixture handler attests registered payloads.
    support::register_payload(&blob);
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;

    let pool_id = B256::repeat_byte(0x48);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;
    // Pre-populate the blob so the ranged request is a cache hit.
    cache.insert_blob(blob.clone()).await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let req_off = 16 * 1024u64;
    // `byte_offset` in bounds but `byte_offset + byte_len` well past the blob
    // end (`blob_size`, not `blob_size - req_off`): must clamp, not refuse.
    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        req_off,
        blob_size,
        RATE_PER_MB,
    )
    .await?;

    let want = blob
        .get(usize::try_from(req_off)?..)
        .ok_or_else(|| anyhow::anyhow!("offset past blob"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "an end past the blob must deliver clamped to the blob's end, got {} of {} bytes",
        got.len(),
        want.len()
    );
    // The clamp is not a range-not-satisfiable rejection: the reject-path
    // counter must stay untouched.
    anyhow::ensure!(
        counter_value(
            &metrics,
            "serve_stream_rejected_range_not_satisfiable_total"
        )? == 0,
        "a clamped end must not fire the range-not-satisfiable reject path"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Parse an inclusive-end HTTP byte-range header (`bytes=START-END`, the shape
/// `HttpOrigin::fetch_range_data` emits) into `(start, end_inclusive)`.
fn parse_byte_range(h: &str) -> Option<(u64, u64)> {
    let (a, b) = h.strip_prefix("bytes=")?.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// A WHOLE-BLOB cold miss whose own origin publishes the `{H}.obao4` outboard is
/// served through the own-origin two-leg path (`serve_via_backend_origin`):
/// dispatch reads the origin's size proof, signs `ok:true`, and runs the local pull leg (fill the cache from origin)
/// beside the serve leg (stream the filling cache to the paying client). This
/// asserts the dispatch selection reaches `serve_via_backend_origin` — the
/// `decdn_local_outboard_serves_total` tier counter fires once — and that the
/// client receives a coherent, hash-verifying whole-blob delivery. There is no
/// upstream, pool, or payment on the ingest side; the client still pays the
/// downstream vouchers exactly as any paid delivery.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
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
    // (1) the size proof the open answers from.
    mount_size_proof(&server, &hex, &blob).await;
    // (2) sibling outboard GET: the pull leg's first draw reads it, and later
    //     draws reuse the cached copy (#2061).
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

    let pool_id = B256::repeat_byte(0x51);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
        pool_id,
        provider,
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
    // `serve_via_backend_origin` rather than another tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "the own-origin two-leg serve tier must fire once"
    );
    // A tier fired at all only because the availability gate classified this as
    // a miss, so the two statements must agree.
    // Its first frame records into the miss time-to-first-byte sibling too.
    anyhow::ensure!(
        counter_value(&metrics, "serve_cache_miss_total")? == 1
            && counter_value(&metrics, "serve_cache_hit_total")? == 0
            && counter_value(&metrics, "serve_first_byte_miss_seconds_count")? == 1
            && counter_value(&metrics, "serve_first_byte_hit_seconds_count")? == 0,
        "a serve that ran a fill tier must be classified a miss, not a hit"
    );
    // The outboard is read from the origin once per hash: the first draw caches
    // it and every later draw reuses it (#2061).
    let obao_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET" && r.url.path() == format!("/{hex}.obao4")
    })
    .await?;
    anyhow::ensure!(
        obao_gets == 1,
        "the outboard must be read from the origin once, read {obao_gets} times"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    // The operator's own origin is not a peer: the fill counts as bytes served,
    // never as bytes received from another node.
    anyhow::ensure!(
        counter_value(&metrics, "bytes_received_total")? == 0,
        "an own-origin fill must not count as bytes received from peers"
    );
    anyhow::ensure!(
        counter_value(&metrics, "bytes_served_total")? > 0,
        "the served blob must count as bytes served"
    );
    Ok(())
}

/// The shape `decdn get` actually sends on a cold miss:
/// `byte_offset == 0, byte_len == total_bytes` — a BOUNDED request that covers the
/// whole blob. It must take the same two-leg streaming tier as the unbounded
/// `(0, 0)` request: the signed response goes out before the origin download
/// finishes, so a multi-GiB blob does not trip the client's stall clock. The tier
/// counter `local_outboard_serves_total` firing once is the proof of routing; the
/// byte-exact delivery is the proof the range-clamped serve leg is correct.
#[tokio::test(flavor = "multi_thread")]
async fn bounded_whole_blob_own_origin_miss_streams_via_backend_origin() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let aligned = align_range(0, blob_size, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x62);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
        pool_id,
        provider,
        hash,
        0,
        blob_size,
        RATE_PER_MB,
    )
    .await?;

    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "bounded whole-blob delivery mismatch: got {} bytes, want {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "a bounded whole-blob miss must take the own-origin two-leg tier"
    );
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        wholeblob_gets == 0,
        "the spine pulls ranged spans only, saw {wholeblob_gets} un-ranged GET(s)"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// An UNALIGNED bounded request (`byte_offset` inside a chunk group) through the
/// spine. The serve leg must map paid wire back to content from the group-aligned
/// fetch start, not the raw offset, and the client must receive exactly the bytes
/// it asked for (the range decoder trims the aligned superset).
#[tokio::test(flavor = "multi_thread")]
async fn bounded_unaligned_offset_own_origin_miss_streams_the_exact_bytes() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    // 20 KiB is NOT a 16 KiB group boundary; 30 KiB ends mid-group too.
    let (req_off, req_len) = (20 * 1024u64, 30 * 1024u64);
    let aligned = align_range(req_off, req_len, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    anyhow::ensure!(
        a_start == 16 * 1024 && a_end == 64 * 1024,
        "test premise: aligned span"
    );
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x63);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
        pool_id,
        provider,
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
        "unaligned range mismatch: got {} bytes, want {}",
        got.len(),
        want.len()
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "an unaligned bounded miss must take the own-origin two-leg tier"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A header-only `(0, 0)` open dropped as soon as the header lands (the CLI's
/// multi-source size probe, or a resumed whole-blob fetch), then a second open
/// of the same blob, here `(0, total)` — against a MULTI-DRAW blob
/// with a slow origin, so the throwaway's fill is still live when the real open
/// arrives. Observed ordering: the real open's `serve_audit` parks on the store's
/// per-hash slot until the throwaway's current draw is admitted, then probes the
/// origin and claims — attaching to the live fill or owning a fresh one, depending
/// on whether the throwaway's serve leg has torn down yet. Every ordering must give
/// a byte-exact delivery, both opens entering the two-leg tier, and no whole-blob
/// GET. When the real open attaches, no span is fetched twice. When it owns a fresh
/// fill, the teardown has aborted the throwaway's in-flight first draw, and the new
/// owner GETs that draw again: the one duplicate draw a thrown-away open costs
/// (`decdn_client::PrimedSource`). No later draw is fetched twice in either ordering.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn throwaway_open_then_real_open_shares_one_multi_draw_fill() -> anyhow::Result<()> {
    let (blob, outboard, hash) = large_blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // Slow dynamic 206 responder: every draw takes 300 ms, so the throwaway's
    // fill is mid-flight when the real open lands.
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206)
                    .set_body_bytes(body.to_vec())
                    .set_delay(Duration::from_millis(300)),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x64);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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

    let total = throwaway_open(
        &client_ep,
        target.clone(),
        client_node_id,
        &client_eth,
        pool_id,
        hash,
    )
    .await?;
    anyhow::ensure!(total == blob_size, "throwaway header total mismatch");

    // Immediately — no sleep — the real open, exactly as `drive` does.
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        ranged_paid_pull(
            &client_ep,
            target,
            client_node_id,
            &client_eth,
            pool_id,
            provider,
            hash,
            0,
            total,
            RATE_PER_MB,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("real open hung behind the throwaway's fill"))??;
    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "real open after a throwaway must deliver byte-exact"
    );
    anyhow::ensure!(
        cache.has(hash).await?,
        "the real client paid the fill to the end"
    );

    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 2,
        "both the throwaway and the real open must enter the two-leg tier"
    );
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(wholeblob_gets == 0, "saw {wholeblob_gets} un-ranged GET(s)");
    // No span fetched twice in full, except one repeat of the first draw when the
    // real open owns a fresh fill after the throwaway's teardown aborted it.
    let reqs = server
        .received_requests()
        .await
        .ok_or_else(|| anyhow::anyhow!("wiremock request recording disabled"))?;
    let ranges: Vec<String> = reqs
        .iter()
        .filter(|r| r.method.as_str() == "GET" && r.url.path() == format!("/{hex}"))
        .filter_map(|r| r.headers.get("range").and_then(|v| v.to_str().ok()))
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(
        ranges.len() >= 2,
        "test premise: a 3 MiB blob takes several draws, saw {ranges:?}"
    );
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for range in &ranges {
        *counts.entry(range.as_str()).or_default() += 1;
    }
    let repeated: Vec<(&str, usize)> = counts.into_iter().filter(|&(_, n)| n > 1).collect();
    let first_draw_repeated_once = |&(range, n): &(&str, usize)| {
        n == 2 && parse_byte_range(range).is_some_and(|(start, _)| start == 0)
    };
    anyhow::ensure!(
        repeated.len() <= 1 && repeated.iter().all(first_draw_repeated_once),
        "a span other than the aborted first draw was fetched twice in full: {ranges:?}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The bundle-pull splice shape against a STALLED owner: a `(0, 0)` open whose
/// client stays connected but never pays (the CLI's throwaway open before the
/// node notices its close, or any client that stops paying), then the real open
/// for only the CHANGED TAIL at an offset far past anything that fill will draw
/// (its pull parks at the ramp floor with a paid frontier of 0). The tail request
/// must not attach to that fill — `claim_fill` gives a request ahead of a live
/// fill's paid frontier its own pull — and must be served byte-exact well inside a
/// client's 30 s stall budget.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn tail_open_ahead_of_a_stalled_owner_does_not_starve() -> anyhow::Result<()> {
    let (blob, outboard, hash) = large_blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();
    // The changed tail starts 2 MiB in — past the floor draw the throwaway's
    // fill makes from byte 0.
    let tail_off = 2 * 1024 * 1024u64;

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206)
                    .set_body_bytes(body.to_vec())
                    .set_delay(Duration::from_millis(200)),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x66);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    // Concurrent accept loop: the stalled connection stays open for the whole
    // test, so the tail open must be served beside it, as the daemon does.
    let server_task = spawn_server_concurrent(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    let (stalled_conn, total) = idle_open(
        &client_ep,
        target.clone(),
        client_node_id,
        &client_eth,
        pool_id,
        hash,
    )
    .await?;
    anyhow::ensure!(total == blob_size, "idle open header total mismatch");
    // Let the stalled owner's fill make its floor draw and park.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let started = std::time::Instant::now();
    let got = tokio::time::timeout(
        Duration::from_secs(15),
        ranged_paid_pull(
            &client_ep,
            target,
            client_node_id,
            &client_eth,
            pool_id,
            provider,
            hash,
            tail_off,
            0,
            RATE_PER_MB,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the tail open starved behind the throwaway's parked fill"))??;
    let want = blob
        .get(usize::try_from(tail_off)?..)
        .ok_or_else(|| anyhow::anyhow!("tail out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "tail delivery mismatch: got {} bytes, want {}",
        got.len(),
        want.len()
    );
    anyhow::ensure!(
        started.elapsed() < Duration::from_secs(10),
        "the tail must not wait on a stalled fill: took {:?}",
        started.elapsed()
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 2,
        "both the stalled open and the tail open must enter the two-leg tier"
    );
    stalled_conn.close(0u32.into(), b"done");

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The other ordering: the throwaway's fill is already TORN DOWN (last-out cancel,
/// or complete for a small blob) when the real open arrives. The real open must own
/// a fresh fill or hit the cache — never park on the dead session — and deliver
/// byte-exact within a hard timeout.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn throwaway_open_torn_down_before_real_open_still_serves() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206).set_body_bytes(body.to_vec()),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x65);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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

    let total = throwaway_open(
        &client_ep,
        target.clone(),
        client_node_id,
        &client_eth,
        pool_id,
        hash,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let got = tokio::time::timeout(
        Duration::from_secs(20),
        ranged_paid_pull(
            &client_ep,
            target,
            client_node_id,
            &client_eth,
            pool_id,
            provider,
            hash,
            0,
            total,
            RATE_PER_MB,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("real open hung after the throwaway's teardown"))??;
    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "real open after a torn-down throwaway must deliver byte-exact"
    );

    // Never a whole-blob GET, and at most one extra ranged draw for the span the
    // cancelled fill was mid-way through.
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(wholeblob_gets == 0, "saw {wholeblob_gets} un-ranged GET(s)");
    // The cancelled fill's draw and the real open's re-draw can both reach the
    // origin.
    let draws = count_requests(&server, |r: &Request| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        draws <= 2,
        "a torn-down throwaway costs at most one duplicate draw, saw {draws}"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// How a broken own origin answers the reads a two-leg serve makes after
/// admission. Every case publishes a valid `{H}.sizeproof`, so the open
/// answers `ok: true`.
#[derive(Clone, Copy, Debug)]
enum BrokenObject {
    /// `{H}.obao4` answers `404`: the origin does not publish the outboard.
    MissingOutboard,
    /// `{H}.obao4` answers `503`.
    OutboardOutage,
    /// `{H}.obao4` is shorter than the proven size needs.
    TruncatedOutboard,
    /// The data object ignores `Range`: a ranged GET answers `200` with the
    /// whole body.
    NoRangeSupport,
    /// A ranged data GET answers `500`.
    RangeOutage,
    /// `{H}` answers `404`: the origin publishes the size proof and the
    /// outboard but not the data.
    MissingData,
}

/// Mount the origin for `case`: the size proof, the outboard and the data
/// object, one of them broken as `case` says.
async fn mount_broken_origin(
    server: &MockServer,
    case: BrokenObject,
    blob: &[u8],
    outboard: &[u8],
    hex: &str,
) {
    mount_size_proof(server, hex, blob).await;
    let outboard_response = match case {
        BrokenObject::MissingOutboard => ResponseTemplate::new(404),
        BrokenObject::OutboardOutage => ResponseTemplate::new(503),
        BrokenObject::TruncatedOutboard => ResponseTemplate::new(200).set_body_bytes(
            outboard
                .get(..outboard.len().saturating_sub(7))
                .unwrap_or_default(),
        ),
        BrokenObject::NoRangeSupport | BrokenObject::RangeOutage | BrokenObject::MissingData => {
            ResponseTemplate::new(200).set_body_bytes(outboard.to_vec())
        }
    };
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(outboard_response)
        .mount(server)
        .await;
    let data_response = match case {
        BrokenObject::NoRangeSupport => ResponseTemplate::new(200).set_body_bytes(blob.to_vec()),
        BrokenObject::RangeOutage => ResponseTemplate::new(500),
        BrokenObject::MissingData => ResponseTemplate::new(404),
        BrokenObject::MissingOutboard
        | BrokenObject::OutboardOutage
        | BrokenObject::TruncatedOutboard => {
            ResponseTemplate::new(206).set_body_bytes(blob.to_vec())
        }
    };
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .respond_with(data_response)
        .mount(server)
        .await;
}

/// A missing or broken own-origin object is an origin fault (ADR 037 §
/// Origin-tier pull-through). The open answers `ok: true` from the size proof,
/// the payer's first proof admits the stream, and the fill then fails: the
/// node refuses the stream with a `StreamError` for `InternalError` before
/// any `ChunkData`. The node never reads the whole blob
/// from its origin, commits nothing to its store, and reports the fault as
/// its own, never as a cache miss.
///
/// The pull runs under a hard `timeout`: the pull leg's terminal signal must
/// reach the serve leg and end the stream, so a hang fails the test rather than
/// stalling the run.
#[allow(clippy::too_many_lines)] // one linear integration-test scenario
async fn broken_origin_object_faults_before_any_chunk(case: BrokenObject) -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let hex = hash.to_hex();
    let server = MockServer::start().await;
    mount_broken_origin(&server, case, &blob, &outboard, &hex).await;

    let pool_id = B256::repeat_byte(0x6B);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        Some(Duration::from_secs(15)),
    )
    .await?;
    cache.set_origin_read_budget(Duration::from_secs(2), 1_000_000);
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let pulled = tokio::time::timeout(
        Duration::from_secs(20),
        ranged_paid_pull(
            &client_ep,
            EndpointAddr::new(server_id).with_ip_addr(server_addr),
            client_node_id,
            &client_eth,
            pool_id,
            server_eth.address(),
            hash,
            0,
            0,
            RATE_PER_MB,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{case:?}: a broken origin must end the stream, not hang"))?;
    let Err(err) = pulled else {
        anyhow::bail!("{case:?}: a broken origin must not deliver the blob");
    };
    // The open answered `ok: true` from the size proof; the stream then ended
    // with a `StreamError` for `InternalError` (wire `Declined`) before any
    // `ChunkData`.
    anyhow::ensure!(
        !err.to_string().contains("delivery refused"),
        "{case:?}: the open answers from the size proof, got: {err:#}"
    );
    anyhow::ensure!(
        err.to_string().contains("stream error after 0 bytes")
            && err.to_string().contains("Declined"),
        "{case:?}: a StreamError(Declined) before any ChunkData, got: {err:#}"
    );

    anyhow::ensure!(
        !cache.has(hash).await? && cache.present_ranges(hash).await?.held_bytes() == 0,
        "{case:?}: a faulted fill commits nothing"
    );
    let wholeblob_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && !r.headers.contains_key("range")
    })
    .await?;
    anyhow::ensure!(
        wholeblob_gets == 0,
        "{case:?}: the node never reads the whole blob, saw {wholeblob_gets} un-ranged GET(s)"
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "{case:?}: the stream takes the own-origin two-leg tier"
    );
    anyhow::ensure!(
        counter_value(&metrics, "node_pull_local_fault_total")? == 1,
        "{case:?}: the fill fails as a local-origin fault"
    );
    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_cache_miss_total")? == 0,
        "{case:?}: a broken origin must not be reported as an empty node"
    );
    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_internal_error_total")? == 1,
        "{case:?}: the node refuses the stream with a StreamError before any ChunkData"
    );

    // The engine memoised the fault, so the next open refuses at open with no
    // origin read, instead of answering `ok: true` and refusing after the
    // payer's first proof.
    let origin_reads = count_requests(&server, |r| r.method.as_str() == "GET").await?;
    let again = tokio::time::timeout(
        Duration::from_secs(20),
        ranged_paid_pull(
            &client_ep,
            EndpointAddr::new(server_id).with_ip_addr(server_addr),
            client_node_id,
            &client_eth,
            pool_id,
            server_eth.address(),
            hash,
            0,
            0,
            RATE_PER_MB,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{case:?}: the second open must end, not hang"))?;
    let Err(again) = again else {
        anyhow::bail!("{case:?}: a memoised fault must not deliver the blob");
    };
    anyhow::ensure!(
        again.to_string().contains("delivery refused"),
        "{case:?}: the next open refuses at open, got: {again:#}"
    );
    anyhow::ensure!(
        count_requests(&server, |r| r.method.as_str() == "GET").await? == origin_reads,
        "{case:?}: the memoised fault reads no origin object"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A data object that stops serving after the stream has sent bytes is a
/// fault mid-stream, not a refusal: the node already sent `ChunkData`, so it
/// cannot answer with a refusal class. The stream ends without hanging, the
/// node keeps only the verified prefix, the refusal counter stays at zero,
/// and the engine memoises the fault so the next open refuses at open.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)] // one linear integration-test scenario
async fn a_data_object_that_fails_mid_stream_ends_the_stream() -> anyhow::Result<()> {
    // A little over one range window, so the pull draws a second window after
    // the first has streamed.
    let window = usize::try_from(decdn_cache::RANGE_PULL_WINDOW_BYTES)?;
    let blob: Vec<u8> = (0..window + 200 * 1024)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ob.data.clone()))
        .mount(&server)
        .await;
    // The first window serves; every later window answers 404.
    let origin_bytes = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range);
            match span {
                Some((0, end)) => usize::try_from(end)
                    .ok()
                    .and_then(|end| origin_bytes.get(..=end))
                    .map_or_else(
                        || ResponseTemplate::new(416),
                        |body| ResponseTemplate::new(206).set_body_bytes(body.to_vec()),
                    ),
                _ => ResponseTemplate::new(404),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x6C);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
    let pull = || {
        ranged_paid_pull(
            &client_ep,
            EndpointAddr::new(server_id).with_ip_addr(server_addr),
            client_node_id,
            &client_eth,
            pool_id,
            server_eth.address(),
            hash,
            0,
            0,
            RATE_PER_MB,
        )
    };
    let pulled = tokio::time::timeout(Duration::from_secs(30), pull())
        .await
        .map_err(|_| anyhow::anyhow!("a mid-stream fault must end the stream, not hang"))?;
    let Err(err) = pulled else {
        anyhow::bail!("a data object that stops serving must not deliver the blob");
    };
    anyhow::ensure!(
        !err.to_string().contains("delivery refused")
            && !err.to_string().contains("stream error after 0 bytes"),
        "the stream answered ok and sent bytes before it failed, got: {err:#}"
    );
    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_internal_error_total")? == 0,
        "a fault after the first ChunkData is not a refusal"
    );
    anyhow::ensure!(
        !cache.has(hash).await?,
        "the node never holds a blob it could not fill"
    );
    anyhow::ensure!(
        cache.present_ranges(hash).await?.held_bytes() > 0,
        "the node keeps the verified prefix it streamed"
    );

    let again = tokio::time::timeout(Duration::from_secs(20), pull())
        .await
        .map_err(|_| anyhow::anyhow!("the second open must end, not hang"))?;
    anyhow::ensure!(
        again.is_err_and(|e| e.to_string().contains("delivery refused")),
        "the memoised fault refuses the next open at open"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_outboard_faults_before_any_chunk() -> anyhow::Result<()> {
    broken_origin_object_faults_before_any_chunk(BrokenObject::MissingOutboard).await
}

#[tokio::test(flavor = "multi_thread")]
async fn outboard_outage_faults_before_any_chunk() -> anyhow::Result<()> {
    broken_origin_object_faults_before_any_chunk(BrokenObject::OutboardOutage).await
}

#[tokio::test(flavor = "multi_thread")]
async fn truncated_outboard_faults_before_any_chunk() -> anyhow::Result<()> {
    broken_origin_object_faults_before_any_chunk(BrokenObject::TruncatedOutboard).await
}

#[tokio::test(flavor = "multi_thread")]
async fn no_range_origin_faults_before_any_chunk() -> anyhow::Result<()> {
    broken_origin_object_faults_before_any_chunk(BrokenObject::NoRangeSupport).await
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_data_object_faults_before_any_chunk() -> anyhow::Result<()> {
    broken_origin_object_faults_before_any_chunk(BrokenObject::MissingData).await
}

#[tokio::test(flavor = "multi_thread")]
async fn range_read_outage_faults_before_any_chunk() -> anyhow::Result<()> {
    broken_origin_object_faults_before_any_chunk(BrokenObject::RangeOutage).await
}

/// #1129 through the own-origin presence read: a TRANSPORT fault on the own
/// origin's `{H}.sizeproof` read (GET → 500) with no other fill tier configured
/// must terminally refuse as `InternalError` (a degraded node) before any
/// signature, never `NotFound` (an empty one). Guards the dispatch fault latch
/// against `CacheEngine::origin_presence` swallowing the fault into a clean
/// absence.
#[tokio::test(flavor = "multi_thread")]
async fn own_origin_size_proof_fault_refuses_internal_error_not_cache_miss() -> anyhow::Result<()> {
    let (blob, _outboard, hash) = blob_with_outboard();
    let hex = hash.to_hex();
    drop(blob);

    let server = MockServer::start().await;
    // The size-proof read faults (5xx is a transport-class fault, unlike a
    // 404's clean absence). Nothing else is mounted: no fill tier can serve.
    Mock::given(method("GET"))
        .and(path(format!(
            "/{hex}{}",
            decdn_bao_range::SIZE_PROOF_SUFFIX
        )))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x67);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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

    // A bound whole-blob open, read the refusal, close.
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
        pull_through: true,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: now_us(),
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;
    let (resp, _resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(!resp.body.ok, "a faulted node must refuse");
    conn.close(0u32.into(), b"done");

    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_internal_error_total")? == 1,
        "a size-proof transport fault must refuse as InternalError (degraded node)"
    );
    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_cache_miss_total")? == 0,
        "a degraded node must not be reported as an empty one"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The spine's own pre-signature bounds gate, on a COLD miss: an out-of-bounds
/// bounded request against an own origin that proves the size must be refused
/// `RangeNotSatisfiable` before any `ok: true` is signed — and the floor
/// reservation it releases must not leak, so a follow-up in-bounds request on
/// the same handler still serves.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn cold_out_of_bounds_range_is_refused_before_signing() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let aligned = align_range(0, 0, blob_size)?;
    let span = blob.clone();
    let range_val = format!(
        "bytes={}-{}",
        aligned.fetch_start(),
        aligned.fetch_end() - 1
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x69);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, _cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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

    // Out of bounds: offset at the blob end, on a COLD cache (spine gate, not
    // the direct-serve gate).
    let conn = client_ep
        .connect(target.clone(), ALPN_CLIENT)
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
        pull_through: true,
    };
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset: blob_size,
        byte_len: 16 * 1024,
        timestamp_us: now_us(),
    };
    let payload =
        encode_stream_request(&req, Some(&ext)).map_err(|e| anyhow::anyhow!("encode req: {e}"))?;
    write_frame_to(&mut send, &payload).await?;
    let (resp, _resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(!resp.body.ok, "an out-of-bounds cold miss must be refused");
    conn.close(0u32.into(), b"done");

    anyhow::ensure!(
        counter_value(
            &metrics,
            "serve_stream_rejected_range_not_satisfiable_total"
        )? == 1,
        "the spine's pre-signature gate must answer RangeNotSatisfiable"
    );
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "the gate lives inside the spine tier (entered, then refused pre-signature)"
    );

    // The released floor reservation must not leak: an in-bounds request on the
    // SAME handler now serves the whole blob.
    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        0,
        0,
        RATE_PER_MB,
    )
    .await?;
    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "an in-bounds request after the refusal must still serve"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// The span-capped floor guard, ACCEPT direction: a pool that covers one small
/// aligned span but not a full credit window must be SERVED for a bounded
/// request (the guard prices the span). Guards the `.min(credit_floor)` cap — a
/// regression to the uncapped floor silently refuses every small ranged
/// request from modest pools.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn span_capped_floor_accepts_a_small_range_a_window_poor_pool() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    // A small aligned span: 3 groups = 48 KiB. min_payment(48 KiB, rate 10) = 1;
    // min_payment(1 MiB window, rate 10) = 10. remaining = 5 sits between.
    let (req_off, req_len) = (16 * 1024u64, 48 * 1024u64);
    let aligned = align_range(req_off, req_len, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x6A);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();

    // Build the handler with a pool view whose remaining (5 µUSDC) covers the
    // 48 KiB span (1 µUSDC) but not a one-chunk window (10 µUSDC). M is 0 in
    // this fixture.
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id,
        client_eth.address(),
        provider,
        U256::from(10_000_000u64),
        u64::MAX,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let mut status = std::collections::HashMap::new();
    status.insert(
        pool_id,
        decdn_node::pool_view::PoolStatus {
            owner: client_eth.address(),
            remaining: U256::from(5u64),
            lifecycle: decdn_node::pool_view::Lifecycle::Open,
        },
    );
    let pool_view =
        Arc::new(StatusMapPoolView { status }) as Arc<dyn decdn_node::pool_view::PoolView>;
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache,
        store_dyn,
        RATE_PER_MB,
        &domains(),
        16,
        |deps| support::set_pool_view(deps, pool_view),
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // ACCEPT: the bounded request is served, span-priced.
    let got = ranged_paid_pull(
        &client_ep,
        target.clone(),
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
    let want = blob
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "a span the pool can cover must be served"
    );
    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_insufficient_deposit_total")? == 0,
        "the span-capped floor must not refuse a coverable range"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// [`decdn_node::pool_view::PoolView`] over a fixed map, for the floor-guard
/// tests.
#[derive(Debug)]
struct StatusMapPoolView {
    status: std::collections::HashMap<B256, decdn_node::pool_view::PoolStatus>,
}

#[async_trait::async_trait]
impl decdn_node::pool_view::PoolView for StatusMapPoolView {
    async fn status(&self, pool_id: B256) -> Option<decdn_node::pool_view::PoolStatus> {
        self.status.get(&pool_id).copied()
    }
}

/// A distinctive 3 MiB payload plus its outboard: larger than `PULL_WINDOW_FLOOR`,
/// so a whole-blob own-origin miss takes several pull-leg draws.
fn large_blob_with_outboard() -> (Vec<u8>, Vec<u8>, Hash) {
    let blob: Vec<u8> = (0..3 * 1024 * 1024u32)
        .map(|i| u8::try_from(i.wrapping_mul(2_654_435_761) >> 24).unwrap_or(0))
        .collect();
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    (blob, ob.data, hash)
}

/// INTERIOR-HOLD own-origin serve-miss. The node already holds an aligned
/// INTERIOR range of the blob before the whole-blob request arrives; the local pull
/// leg must draw ONLY the surrounding gaps from origin (never the held interior),
/// the serve leg's coherent whole-range encoder must seed the shared outboard from
/// the held range's proof nodes (without that seed it would park on the held span's
/// proof node and hang), and the paying client must still receive the whole blob
/// byte-exact and in order.
///
/// This is the key new coverage: it proves the range-minimized pull + the
/// shared-outboard held-range SEED (`window.rs` `outboard_pairs` pre-seed) both work
/// together, which the full-miss own-origin test cannot exercise (a full miss holds
/// nothing, so the seed is a no-op and every byte is pulled).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn interior_hold_own_origin_miss_pulls_only_the_gaps() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    // The held interior spans a middle band of chunk groups: [64 KiB, 128 KiB) is
    // 16 KiB-group aligned, so admitting it verbatim leaves real gaps on both sides
    // — a prefix [0, 64 KiB) and a suffix [128 KiB, 200 KiB).
    let held_off = 64 * 1024u64;
    let held_len = 64 * 1024u64;
    let held_aligned = align_range(held_off, held_len, blob_size)?;
    let (held_start, held_end) = (held_aligned.fetch_start(), held_aligned.fetch_end());
    let held_data = blob
        .get(usize::try_from(held_start)?..usize::try_from(held_end)?)
        .ok_or_else(|| anyhow::anyhow!("held span out of bounds"))?
        .to_vec();

    let server = MockServer::start().await;
    // (1) the size proof the open answers from.
    mount_size_proof(&server, &hex, &blob).await;
    // (2) sibling outboard GET: the pull leg's range-encode outboard fetch.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // (3) ranged data GET → a DYNAMIC 206 that serves exactly whatever inclusive
    //     byte range the origin asks for, sliced out of the blob. Using a dynamic
    //     responder (rather than one exact-range mock per gap) means the assertion,
    //     not the mock's match arms, decides which spans are legitimate — a request
    //     that touched the held interior would still be recorded, then caught below.
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206).set_body_bytes(body.to_vec()),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x52);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    // Seed the INTERIOR range into the same cache the handler serves from (the
    // engine handle is shared), so the pull leg's `missing_ranges(0, 0)` returns
    // only the two surrounding gaps. Mirrors how the cache's own `admit_bao` tests
    // seed a partial: `encode_verified_range` yields the combined wire `admit_bao`
    // imports under `H`.
    let held_bao = encode_verified_range(
        *hash.as_bytes(),
        &held_aligned,
        &held_data,
        bytes::Bytes::from(outboard.clone()),
    )
    .map_err(|e| anyhow::anyhow!("encode held range: {e:?}"))?;
    cache
        .admit_bao(hash, held_aligned.chunk_ranges().clone(), held_bao)
        .await?;
    anyhow::ensure!(
        !cache.present_ranges(hash).await?.is_complete(),
        "the seeded interior range must leave the blob a partial, not complete"
    );
    anyhow::ensure!(
        !cache.has(hash).await?,
        "an interior partial must not read as a full holder"
    );

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);

    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Whole blob (offset == 0 && len == 0) → the own-origin two-leg path.
    let got = ranged_paid_pull(
        &client_ep,
        target,
        client_node_id,
        &client_eth,
        pool_id,
        provider,
        hash,
        0,
        0,
        RATE_PER_MB,
    )
    .await?;

    // 1) The client got the WHOLE blob, byte-exact and in order — the held interior
    //    (read locally + seeded into the shared outboard) and the pulled gaps
    //    reassembled into one coherent, hash-verifying stream.
    anyhow::ensure!(
        got.as_slice() == blob.as_slice(),
        "interior-hold whole-blob delivery mismatch: got {} bytes, want {}",
        got.len(),
        blob.len()
    );

    // 2) The origin served the GAPS and NOT the held interior. Any ranged GET whose
    //    inclusive span `[s, e]` overlaps the held `[held_start, held_end)` is a
    //    re-pull of bytes the node already had — the range-minimization bug this
    //    test exists to catch.
    let held_overlap_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .is_some_and(|(s, e)| s < held_end && e >= held_start)
    })
    .await?;
    anyhow::ensure!(
        held_overlap_gets == 0,
        "the origin must never re-fetch the held interior range, saw {held_overlap_gets} \
         overlapping ranged GET(s)"
    );

    // The prefix gap [0, held_start) and the suffix gap [held_end, blob_size) were
    // each drawn from origin — proof the gaps really were pulled (not silently
    // skipped, which would also produce zero held-overlap GETs).
    let prefix_gap_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .is_some_and(|(s, _e)| s < held_start)
    })
    .await?;
    let suffix_gap_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .is_some_and(|(s, _e)| s >= held_end)
    })
    .await?;
    anyhow::ensure!(
        prefix_gap_gets >= 1,
        "the prefix gap [0, {held_start}) must be pulled from origin"
    );
    anyhow::ensure!(
        suffix_gap_gets >= 1,
        "the suffix gap [{held_end}, {blob_size}) must be pulled from origin"
    );

    // 3) Dispatch selected the own-origin two-leg tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "the own-origin two-leg serve tier must fire once"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// A BOUNDED own-origin serve-miss over a node that holds ranges both inside and
/// outside the request (#2205). The serve seeds the shared outboard only from the
/// held part of its own aligned range. The held groups 4-7 form one whole subtree,
/// so that subtree's inner proof nodes lie on no path to a pulled group: only the
/// seed supplies them. Without them the encoder parks and the serve fails when the
/// pull ends. The held head and tail groups lie outside the request and need no
/// seed. The client still gets its range byte-exact, and the origin serves only the
/// request's two gap groups.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn a_bounded_miss_seeds_only_the_held_ranges_inside_it() -> anyhow::Result<()> {
    const G: u64 = 16 * 1024;
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206).set_body_bytes(body.to_vec()),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x53);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
        client_eth.address(),
        &server_eth,
        server_id,
        None,
    )
    .await?;

    // The 200 KiB blob spans groups 0-12 (group 12 is a half group). Hold the head
    // group, groups 4-7 and the tail group.
    let held = [(0, G), (4 * G, 4 * G), (12 * G, blob_size - 12 * G)];
    for (off, len) in held {
        let aligned = align_range(off, len, blob_size)?;
        let data = blob
            .get(usize::try_from(aligned.fetch_start())?..usize::try_from(aligned.fetch_end())?)
            .ok_or_else(|| anyhow::anyhow!("held span out of bounds"))?
            .to_vec();
        let bao = encode_verified_range(
            *hash.as_bytes(),
            &aligned,
            &data,
            bytes::Bytes::from(outboard.clone()),
        )
        .map_err(|e| anyhow::anyhow!("encode held range: {e:?}"))?;
        cache
            .admit_bao(hash, aligned.chunk_ranges().clone(), bao)
            .await?;
    }

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // [3G + 100, 9G - 50) aligns to groups 3-8: held 4-7 inside, gaps 3 and 8.
    let (req_off, req_len) = (3 * G + 100, 6 * G - 150);
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        ranged_paid_pull(
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
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the bounded miss over held ranges hung"))??;
    let want = blob
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(
        got.as_slice() == want,
        "bounded miss over held ranges must be byte-exact"
    );

    // The origin served the two gap groups and nothing the node held.
    let ranged_gets: Vec<(u64, u64)> = server
        .received_requests()
        .await
        .ok_or_else(|| anyhow::anyhow!("wiremock request recording disabled"))?
        .iter()
        .filter(|r| r.method.as_str() == "GET" && r.url.path() == format!("/{hex}"))
        .filter_map(|r| {
            r.headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
        })
        .collect();
    for (s, e) in &ranged_gets {
        anyhow::ensure!(
            !(*s < 8 * G && *e >= 4 * G),
            "the origin must never re-fetch held groups 4-7, saw bytes={s}-{e}"
        );
    }
    for gap in [3 * G, 8 * G] {
        anyhow::ensure!(
            ranged_gets.iter().any(|(s, e)| *s <= gap && *e >= gap),
            "gap group at {gap} must be pulled from origin, saw {ranged_gets:?}"
        );
    }
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 1,
        "the own-origin two-leg serve tier must fire once"
    );

    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// Spawn a server that handles each inbound connection on its OWN task, so two
/// concurrent `cdn/client/v1` requests are served in parallel. The default
/// `support::spawn_server` awaits `handler.accept` INLINE in its accept loop, so a
/// long-running first serve blocks the loop and the second connection's `connect`
/// times out — which also means the second request would never reach the fill
/// registry (`claim_fill`) while the first still owns the live fill, defeating the
/// coalescing this test exercises. Mirrors `spawn_server_concurrent` in
/// `node_origin_pull.rs`.
fn spawn_server_concurrent(
    server_ep: iroh::Endpoint,
    handler: Arc<ClientHandler>,
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

/// Build a `ClientHandler` over an `HttpOrigin` with TWO independently-funded
/// pools (one per concurrent client). Two pools (not one) so each of the two
/// concurrent deliveries has its own lane with monotonic voucher accounting; the
/// fill registry keys coalescing on the HASH, not the lane. The pull-through deadline
/// is threaded through unchanged (own-origin coalescing does not use it, but the
/// helper is shared).
#[allow(clippy::too_many_arguments)]
async fn handler_two_pools_over_http_origin(
    origin_uri: &str,
    owner_pool: B256,
    client_a: Address,
    waiter_pool: B256,
    client_b: Address,
    server_eth: &Arc<PrivateKeySigner>,
    server_id: iroh::PublicKey,
    pull_through: Option<Duration>,
) -> anyhow::Result<(
    Arc<ClientHandler>,
    CacheEngine,
    Arc<Metrics>,
    tempfile::TempDir,
)> {
    let store = Arc::new(MemoryPoolStateStore::new());
    for (pool_id, client) in [(owner_pool, client_a), (waiter_pool, client_b)] {
        store.record(&LaneState::hydrate(
            pool_id,
            client,               // capability signer
            server_eth.address(), // provider
            U256::from(10_000_000u64),
            u64::MAX,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        ))?;
    }

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(origin_uri)?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let handler = build_handler_full_configured(
        server_id,
        server_eth,
        &metrics,
        limiter,
        cache.clone(),
        store_dyn,
        RATE_PER_MB,
        &domains(),
        16,
        |deps| deps.pull_through = pull_through,
    )?;
    Ok((handler, cache, metrics, cache_dir))
}

/// TWO concurrent whole-blob own-origin misses for the SAME hash COALESCE to a
/// single origin ranged GET (#1621, `CacheEngine::claim_fill`). Both requests enter
/// `serve_via_backend_origin`; under one registry lock the first OWNS the local
/// origin pull and the second ATTACHES as an observer, streaming the SAME filling
/// cache to its own client. So the node eats the S3 egress ONCE — the headline
/// own-origin saving — while running TWO live serve legs rather than parking the
/// second request until the first completes. The proof is on the ORIGIN side:
/// exactly ONE ranged `206` data GET reaches the backend (not two), and BOTH
/// clients receive the whole blob byte-exact on their own lanes. The own-origin
/// serve tier therefore fires TWICE (both legs genuinely serve via the
/// backend-origin path), not once.
///
/// The race is forced deterministically: the origin's ranged data GET is delayed,
/// so the owner holds the fill across a wide window; the second is launched after a
/// short stagger, guaranteeing it observes the live fill and attaches.
/// The ATTACH arm of the #2062 rule end-to-end: while a PAYING owner's
/// multi-draw whole-blob fill is live, a second client opens a bounded range
/// at a small offset the owner's paid frontier has already cleared. The claim
/// must attach (the skip counter stays 0), the overlap must not be fetched
/// twice (every ranged GET carries a distinct Range), both deliveries are
/// byte-exact, and the attached leg's payments flow through the guarded
/// `extend_served_from` without wedging either client.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn mid_blob_open_behind_a_paying_owners_frontier_attaches() -> anyhow::Result<()> {
    let (blob, outboard, hash) = large_blob_with_outboard();
    let hex = hash.to_hex();

    let server = MockServer::start().await;
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // Slow dynamic 206 responder: each draw takes 300 ms, so the owner's fill
    // is still mid-flight after its first voucher, when the second open lands.
    let blob_for_resp = blob.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header_exists("range"))
        .respond_with(move |req: &Request| {
            let span = req
                .headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_byte_range)
                .and_then(|(s, e)| Some((usize::try_from(s).ok()?, usize::try_from(e).ok()?)))
                .and_then(|(s, e)| blob_for_resp.get(s..=e));
            match span {
                Some(body) => ResponseTemplate::new(206)
                    .set_body_bytes(body.to_vec())
                    .set_delay(Duration::from_millis(300)),
                None => ResponseTemplate::new(416),
            }
        })
        .mount(&server)
        .await;

    let owner_pool = B256::repeat_byte(0x6C);
    let waiter_pool = B256::repeat_byte(0x6D);
    let owner_eth = Arc::new(PrivateKeySigner::random());
    let waiter_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    // Built inline rather than via `handler_two_pools_over_http_origin`: the
    // discriminating counter (`fill_not_coalesced`) lives on `CacheMetrics`,
    // which that fixture does not wire.
    let store = Arc::new(MemoryPoolStateStore::new());
    for (pool_id, client) in [
        (owner_pool, owner_eth.address()),
        (waiter_pool, waiter_eth.address()),
    ] {
        store.record(&LaneState::hydrate(
            pool_id,
            client,
            server_eth.address(),
            U256::from(10_000_000u64),
            u64::MAX,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        ))?;
    }
    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let cache_metrics = Arc::new(decdn_cache::CacheMetrics::default());
    let cache = CacheEngine::open_full(
        cache_dir.path(),
        vec![origin as Arc<dyn Origin>],
        16,
        decdn_cache::PinnedHashes::default(),
        decdn_cache::RetryPolicy::default(),
        decdn_cache::CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cache_metrics)),
        Duration::ZERO,
    )
    .await?;
    let metrics = Arc::new(Metrics::new());
    let limiter = permissive_limiter(&metrics);
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        limiter,
        cache.clone(),
        store_dyn,
        RATE_PER_MB,
        &domains(),
        16,
        |_| {},
    )?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server_concurrent(server_ep.clone(), handler);

    let owner_sk = fresh_key();
    let owner_node_id = B256::from(*owner_sk.public().as_bytes());
    let (owner_ep, _) = local_endpoint(owner_sk, vec![]).await?;
    let waiter_sk = fresh_key();
    let waiter_node_id = B256::from(*waiter_sk.public().as_bytes());
    let (waiter_ep, _) = local_endpoint(waiter_sk, vec![]).await?;
    let owner_target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let waiter_target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // The owner streams (and pays for) the whole blob.
    let owner_hash = hash;
    let owner_eth2 = Arc::clone(&owner_eth);
    let owner_ep_task = owner_ep.clone();
    let a = tokio::spawn(async move {
        ranged_paid_pull(
            &owner_ep_task,
            owner_target,
            owner_node_id,
            &owner_eth2,
            owner_pool,
            provider,
            owner_hash,
            0,
            0,
            RATE_PER_MB,
        )
        .await
    });
    // Wait until the node accepts the owner's first voucher, then open a small
    // range at one group in, far behind the paid frontier. That voucher pays a
    // full `CHUNK_BYTES` interval, and the serve leg raises the fill's paid
    // frontier past it with no await after `decdn_vouchers_received_total`
    // ticks. So the counter is the signal that the frontier has cleared
    // `req_off`. The owner is the only payer yet, so every accepted voucher is
    // its own.
    let first_voucher = async {
        while counter_value(&metrics, "vouchers_received_total")? == 0 {
            anyhow::ensure!(!a.is_finished(), "the owner ended before its first voucher");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(30), first_voucher)
        .await
        .map_err(|_| anyhow::anyhow!("the owner's first voucher never landed"))??;
    let (req_off, req_len) = (16 * 1024u64, 32 * 1024u64);
    let got_waiter = ranged_paid_pull(
        &waiter_ep,
        waiter_target,
        waiter_node_id,
        &waiter_eth,
        waiter_pool,
        provider,
        hash,
        req_off,
        req_len,
        RATE_PER_MB,
    )
    .await?;
    let got_owner = a.await??;

    anyhow::ensure!(got_owner.as_slice() == blob.as_slice(), "owner byte-exact");
    let want = blob
        .get(usize::try_from(req_off)?..usize::try_from(req_off + req_len)?)
        .ok_or_else(|| anyhow::anyhow!("requested range out of bounds"))?;
    anyhow::ensure!(got_waiter.as_slice() == want, "attached range byte-exact");
    anyhow::ensure!(cache.has(hash).await?, "the owner's fill completes");

    // The attach arm fired, not the #2062 skip: nothing declined coalescing…
    anyhow::ensure!(
        cache_metrics.fill_not_coalesced.get() == 0,
        "a request behind the paying owner's frontier must attach, not own"
    );
    // …and the overlap was fetched once: every ranged GET carries a distinct
    // Range (the owner's draw sequence), no re-fetch for the waiter.
    let reqs = server
        .received_requests()
        .await
        .ok_or_else(|| anyhow::anyhow!("wiremock request recording disabled"))?;
    let ranges: Vec<String> = reqs
        .iter()
        .filter(|r| r.method.as_str() == "GET" && r.url.path() == format!("/{hex}"))
        .filter_map(|r| r.headers.get("range").and_then(|v| v.to_str().ok()))
        .map(str::to_owned)
        .collect();
    let mut distinct = ranges.clone();
    distinct.sort();
    distinct.dedup();
    anyhow::ensure!(
        distinct.len() == ranges.len(),
        "the overlap must not be fetched twice: {ranges:?}"
    );

    shutdown([server_task], [&owner_ep, &waiter_ep, &server_ep]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn concurrent_whole_blob_own_origin_misses_coalesce_to_one_pull() -> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let blob_size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
    let hex = hash.to_hex();

    let aligned = align_range(0, 0, blob_size)?;
    let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
    let span = blob
        .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
        .to_vec();
    let range_val = format!("bytes={a_start}-{}", a_end - 1);

    let server = MockServer::start().await;
    // The size proof the open answers from, and the sibling outboard the pull
    // leg's range encode reads.
    mount_size_proof(&server, &hex, &blob).await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
        .mount(&server)
        .await;
    // The ranged data GET — DELAYED so the owning request holds the live fill
    // across a wide window while the second request attaches to it. If coalescing
    // is absent, BOTH requests reach here and this mock records TWO matching GETs;
    // the assertion below then fails.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .and(header("range", range_val.as_str()))
        .respond_with(
            ResponseTemplate::new(206)
                .set_body_bytes(span.clone())
                .set_delay(Duration::from_millis(1_200)),
        )
        .mount(&server)
        .await;

    // Two clients, two independently-funded pools: coalescing keys on the hash,
    // and separate lanes keep each delivery's vouchers monotonic.
    let owner_pool = B256::repeat_byte(0x61);
    let waiter_pool = B256::repeat_byte(0x62);
    let owner_eth = Arc::new(PrivateKeySigner::random());
    let waiter_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    // A generous pull-through deadline so the coalesced waiter WAITS on the
    // in-flight entry (populate) instead of a bare presence check.
    let (handler, cache, metrics, _cache_tmp) = handler_two_pools_over_http_origin(
        &server.uri(),
        owner_pool,
        owner_eth.address(),
        waiter_pool,
        waiter_eth.address(),
        &server_eth,
        server_id,
        Some(Duration::from_secs(10)),
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server_concurrent(server_ep.clone(), handler);

    let owner_sk = fresh_key();
    let owner_node_id = B256::from(*owner_sk.public().as_bytes());
    let (owner_ep, _) = local_endpoint(owner_sk, vec![]).await?;
    let waiter_sk = fresh_key();
    let waiter_node_id = B256::from(*waiter_sk.public().as_bytes());
    let (waiter_ep, _) = local_endpoint(waiter_sk, vec![]).await?;
    let owner_target = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let waiter_target = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Request A (the owner) is launched first and, after a short stagger, request B
    // — by then A holds the `claim_fill` Owner claim and is blocked on the delayed
    // origin GET, so B is guaranteed to Attach to A's in-flight fill.
    let owner_hash = hash;
    let a = tokio::spawn(async move {
        ranged_paid_pull(
            &owner_ep,
            owner_target,
            owner_node_id,
            &owner_eth,
            owner_pool,
            provider,
            owner_hash,
            0,
            0,
            RATE_PER_MB,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let got_waiter = ranged_paid_pull(
        &waiter_ep,
        waiter_target,
        waiter_node_id,
        &waiter_eth,
        waiter_pool,
        provider,
        hash,
        0,
        0,
        RATE_PER_MB,
    )
    .await?;
    let got_owner = a.await??;

    // Both clients received the whole blob, byte-exact.
    anyhow::ensure!(
        got_owner.as_slice() == blob.as_slice(),
        "owner delivery mismatch: got {} bytes, want {}",
        got_owner.len(),
        blob.len()
    );
    anyhow::ensure!(
        got_waiter.as_slice() == blob.as_slice(),
        "coalesced delivery mismatch: got {} bytes, want {}",
        got_waiter.len(),
        blob.len()
    );

    // The coalescing proof: the origin served the whole-blob gap exactly ONCE. A
    // second, non-coalesced own-origin pull would draw the same ranged span again.
    let ranged_gets = count_requests(&server, |r| {
        r.method.as_str() == "GET"
            && r.url.path() == format!("/{hex}")
            && r.headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v == range_val)
    })
    .await?;
    anyhow::ensure!(
        ranged_gets == 1,
        "expected exactly ONE origin ranged GET (coalesced), saw {ranged_gets}"
    );

    // BOTH requests ran `serve_via_backend_origin` (owner + attached observer), so
    // the own-origin serve tier fires TWICE — the second is a live serve leg over
    // the shared fill, not a park-and-wait. The coalescing win is the origin count
    // above (ONE fetch), not the serve-tier count.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 2,
        "both misses run the own-origin two-leg serve tier (owner + attached observer)"
    );

    // The blob is fully present after both deliveries.
    anyhow::ensure!(
        cache.has(hash).await?,
        "blob must be present after coalesced serve"
    );

    shutdown([server_task], [&server_ep]).await?;
    Ok(())
}

/// Two concurrent own-origin misses for DISJOINT content (two DIFFERENT hashes)
/// must NOT be wedged onto one fill by the coalescing registry: each opens its own
/// origin fetch, both serve byte-exact, and neither hangs. This is the disjoint
/// twin of `concurrent_whole_blob_own_origin_misses_coalesce_to_one_pull`.
///
/// Two distinct hashes keep the two fills independent at the registry layer;
/// disjoint RANGES of one hash coalesce per `fill_session.rs`'s
/// `claim_disjoint_both_own` / `disjoint_halves_do_not_attach`. The proof here: the origin serves EACH hash's
/// ranged span exactly once (TWO fetches, one per hash — not one shared, not
/// double), both clients receive their whole blob byte-exact, and the whole race
/// completes inside a hard timeout (no wedge / no deadlock).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn two_concurrent_disjoint_own_origin_misses_two_fetches_no_wedge() -> anyhow::Result<()> {
    // Two DISTINCT blobs → two distinct hashes (different length + pattern).
    let (blob_a, outboard_a, hash_a) = blob_with_outboard();
    let blob_b: Vec<u8> = (0..190 * 1024u32)
        .map(|i| u8::try_from((i % 251).wrapping_add(7)).unwrap_or(0))
        .collect();
    let ob_b = PreOrderMemOutboard::create(&blob_b, IROH_BLOCK_SIZE);
    let hash_b = Hash::from_bytes(*ob_b.root.as_bytes());
    let outboard_b = ob_b.data;
    anyhow::ensure!(hash_a != hash_b, "the two blobs must have distinct hashes");

    let server = MockServer::start().await;
    // Mount the size proof + sibling-outboard GET + a DELAYED ranged data GET for
    // each hash, so both fills are in flight concurrently.
    let mut range_vals = Vec::new();
    for (blob, outboard, hash) in [
        (&blob_a, &outboard_a, hash_a),
        (&blob_b, &outboard_b, hash_b),
    ] {
        let size = u64::try_from(blob.len()).unwrap_or(u64::MAX);
        let hex = hash.to_hex();
        let aligned = align_range(0, 0, size)?;
        let (a_start, a_end) = (aligned.fetch_start(), aligned.fetch_end());
        let span = blob
            .get(usize::try_from(a_start)?..usize::try_from(a_end)?)
            .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?
            .to_vec();
        let range_val = format!("bytes={a_start}-{}", a_end - 1);
        mount_size_proof(&server, &hex, blob).await;
        Mock::given(method("GET"))
            .and(path(format!("/{hex}.obao4")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard.clone()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/{hex}")))
            .and(header("range", range_val.as_str()))
            .respond_with(
                ResponseTemplate::new(206)
                    .set_body_bytes(span)
                    .set_delay(Duration::from_millis(600)),
            )
            .mount(&server)
            .await;
        range_vals.push((hex, range_val));
    }

    let pool_a = B256::repeat_byte(0x71);
    let pool_b = B256::repeat_byte(0x72);
    let eth_a = Arc::new(PrivateKeySigner::random());
    let eth_b = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let (handler, cache, metrics, _cache_tmp) = handler_two_pools_over_http_origin(
        &server.uri(),
        pool_a,
        eth_a.address(),
        pool_b,
        eth_b.address(),
        &server_eth,
        server_id,
        Some(Duration::from_secs(10)),
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server_concurrent(server_ep.clone(), handler);

    let sk_a = fresh_key();
    let node_a = B256::from(*sk_a.public().as_bytes());
    let (ep_a, _) = local_endpoint(sk_a, vec![]).await?;
    let sk_b = fresh_key();
    let node_b = B256::from(*sk_b.public().as_bytes());
    let (ep_b, _) = local_endpoint(sk_b, vec![]).await?;
    let target_a = EndpointAddr::new(server_id).with_ip_addr(server_addr);
    let target_b = EndpointAddr::new(server_id).with_ip_addr(server_addr);

    // Launch both concurrently — distinct hashes never coalesce, so no stagger.
    let first_signer = Arc::clone(&eth_a);
    let a = tokio::spawn(async move {
        ranged_paid_pull(
            &ep_a,
            target_a,
            node_a,
            &first_signer,
            pool_a,
            provider,
            hash_a,
            0,
            0,
            RATE_PER_MB,
        )
        .await
    });
    let second_signer = Arc::clone(&eth_b);
    let b = tokio::spawn(async move {
        ranged_paid_pull(
            &ep_b,
            target_b,
            node_b,
            &second_signer,
            pool_b,
            provider,
            hash_b,
            0,
            0,
            RATE_PER_MB,
        )
        .await
    });

    // Hard timeout: a coalescing bug that wedged these two disjoint fills onto one
    // shared session would deadlock — the timeout turns that into a readable failure
    // instead of a CI-timeout hang.
    let (got_a, got_b) = tokio::time::timeout(Duration::from_secs(30), async {
        anyhow::Ok((a.await??, b.await??))
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!("disjoint concurrent serves did not complete — possible wedge")
    })??;

    anyhow::ensure!(
        got_a.as_slice() == blob_a.as_slice(),
        "hash A delivery mismatch: got {} bytes, want {}",
        got_a.len(),
        blob_a.len()
    );
    anyhow::ensure!(
        got_b.as_slice() == blob_b.as_slice(),
        "hash B delivery mismatch: got {} bytes, want {}",
        got_b.len(),
        blob_b.len()
    );

    // Each hash was fetched from origin exactly once — TWO fetches total, proving
    // the fills did NOT coalesce (that would be one) and did NOT double-fetch.
    for (hex, range_val) in &range_vals {
        let gets = count_requests(&server, |r| {
            r.method.as_str() == "GET"
                && r.url.path() == format!("/{hex}")
                && r.headers
                    .get("range")
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v == range_val.as_str())
        })
        .await?;
        anyhow::ensure!(
            gets == 1,
            "hash /{hex} must be fetched exactly once (own pull), saw {gets}"
        );
    }

    // Both misses ran the own-origin two-leg serve tier.
    anyhow::ensure!(
        counter_value(&metrics, "local_outboard_serves_total")? == 2,
        "both disjoint misses run the own-origin serve tier"
    );
    anyhow::ensure!(
        cache.has(hash_a).await? && cache.has(hash_b).await?,
        "both blobs must be present after their serves"
    );

    shutdown([server_task], [&server_ep]).await?;
    Ok(())
}

/// A bound ext for `client_eth` on the connection of `client_node_id`, with
/// `pull_through` as given.
fn bound_ext(
    client_eth: &PrivateKeySigner,
    client_node_id: B256,
    pull_through: bool,
) -> anyhow::Result<StreamRequestExt> {
    let binding_hash =
        binding_signing_hash(client_node_id, EPHEMERAL_BINDING_NONCE, &binding_dom());
    Ok(StreamRequestExt {
        binding: Some(ClientBinding {
            ethereum_address: client_eth.address().into(),
            binding_signature: client_eth
                .sign_hash_sync(&binding_hash)?
                .as_bytes()
                .to_vec(),
        }),
        capability: None,
        pull_through,
    })
}

/// The own-origin presence read that runs before admission goes through the
/// presence memo: two opens for the same hash the origin lacks reach the origin
/// with one `{H}.sizeproof` read, and both answer `NotFound`.
#[tokio::test(flavor = "multi_thread")]
async fn repeat_opens_for_an_absent_hash_read_the_origin_once() -> anyhow::Result<()> {
    let hash = Hash::new(b"a blob no origin holds");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let pool_id = B256::repeat_byte(0x7A);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let (handler, _cache, _metrics, _cache_tmp) = handler_over_http_origin(
        &server.uri(),
        pool_id,
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
    let conn = client_ep
        .connect(
            EndpointAddr::new(server_id).with_ip_addr(server_addr),
            ALPN_CLIENT,
        )
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let ext = bound_ext(&client_eth, client_node_id, false)?;

    for _ in 0..2 {
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
        let req = StreamRequest {
            hash: *hash.as_bytes(),
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
            pool_id: pool_id.into(),
            byte_offset: 0,
            byte_len: 0,
            timestamp_us: now_us(),
        };
        write_frame_to(&mut send, &encode_stream_request(&req, Some(&ext))?).await?;
        let (resp, resp_ext) = read_stream_response(&mut recv).await?;
        anyhow::ensure!(
            !resp.body.ok
                && resp_ext.error() == Some(&decdn_protocol::client::StreamError::NotFound),
            "an absent hash answers NotFound"
        );
    }
    let size_proof_path = format!("/{}{}", hash.to_hex(), decdn_bao_range::SIZE_PROOF_SUFFIX);
    let reads = count_requests(&server, |_| true).await?;
    let proof_reads = count_requests(&server, |r| r.url.path() == size_proof_path).await?;
    anyhow::ensure!(
        reads == 1 && proof_reads == 1,
        "two opens must cost one origin size-proof read, saw {reads} reads"
    );

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}

/// An own-origin open on a pool that cannot cover the span answers before
/// admission, and its admission refuses `Unfunded` at the first proof, before
/// any `ChunkData`. The fill never starts, and the origin sees no outboard
/// read and no range read: the open reads only the memoised size proof.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn an_underfunded_own_origin_open_is_refused_at_admission_and_reads_no_range()
-> anyhow::Result<()> {
    let (blob, outboard, hash) = blob_with_outboard();
    let hex = hash.to_hex();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard))
        .mount(&server)
        .await;
    mount_size_proof(&server, &hex, &blob).await;

    let pool_id = B256::repeat_byte(0x7B);
    let client_eth = Arc::new(PrivateKeySigner::random());
    let server_sk = fresh_key();
    let server_id = server_sk.public();
    let server_eth = Arc::new(PrivateKeySigner::random());
    let provider = server_eth.address();
    let store = Arc::new(MemoryPoolStateStore::new());
    store.record(&LaneState::hydrate(
        pool_id,
        client_eth.address(),
        provider,
        U256::from(10_000_000u64),
        u64::MAX,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    ))?;
    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let cache = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;
    let metrics = Arc::new(Metrics::new());
    // The whole 200 KiB blob costs 2 at the harness rate; the pool holds 1.
    let mut status = std::collections::HashMap::new();
    status.insert(
        pool_id,
        decdn_node::pool_view::PoolStatus {
            owner: client_eth.address(),
            remaining: U256::from(1u64),
            lifecycle: decdn_node::pool_view::Lifecycle::Open,
        },
    );
    let pool_view =
        Arc::new(StatusMapPoolView { status }) as Arc<dyn decdn_node::pool_view::PoolView>;
    let store_dyn: Arc<dyn PoolStateStore> = store;
    let handler = build_handler_full_configured(
        server_id,
        &server_eth,
        &metrics,
        permissive_limiter(&metrics),
        cache.clone(),
        store_dyn,
        RATE_PER_MB,
        &domains(),
        16,
        |deps| support::set_pool_view(deps, pool_view),
    )?;
    let (server_ep, server_addr) = local_endpoint(server_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), handler);
    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let conn = client_ep
        .connect(
            EndpointAddr::new(server_id).with_ip_addr(server_addr),
            ALPN_CLIENT,
        )
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;
    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        payment_pool: decdn_protocol::PoolAccount::eip155(CHAIN_ID, [0x34; 20]),
        pool_id: pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: now_us(),
    };
    let ext = bound_ext(&client_eth, client_node_id, true)?;
    write_frame_to(&mut send, &encode_stream_request(&req, Some(&ext))?).await?;
    let (resp, _resp_ext) = read_stream_response(&mut recv).await?;
    anyhow::ensure!(resp.body.ok, "the own-origin open answers before admission");
    let gets =
        |r: &Request| r.method.as_str() == "GET" && r.url.path() != format!("/{hex}.sizeproof");

    let first_proof = Voucher {
        pool_id,
        signer: client_eth.address(),
        provider,
        amount: U256::from(1u64),
        bytes_delivered: U256::from(64 * 1024u64),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(client_eth.as_ref(), &voucher_dom())
    .map_err(|e| anyhow::anyhow!("sign voucher: {e}"))?;
    write_client_msg(
        &mut send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&first_proof)?),
    )
    .await?;
    match read_client_msg(&mut recv).await? {
        ClientMessage::StreamError(decdn_protocol::client::StreamError::Unfunded) => {}
        other => anyhow::bail!("expected Unfunded before any ChunkData, got {other:?}"),
    }
    anyhow::ensure!(
        counter_value(&metrics, "serve_stream_rejected_insufficient_deposit_total")? == 1,
        "the refusal counts as insufficient deposit"
    );
    anyhow::ensure!(
        count_requests(&server, gets).await? == 0,
        "neither the open nor the refused admission reads the outboard or a range"
    );
    anyhow::ensure!(
        cache.in_flight_total(hash).is_none() && !cache.has(hash).await?,
        "the refused open starts no fill"
    );

    conn.close(0u32.into(), b"done");
    shutdown([server_task], [&client_ep, &server_ep]).await?;
    Ok(())
}
