//! G-NODE-08: authorized-origin recognition (opaque backend fetch).
//!
//! One node, one opaque `fs` backend, one config — the *only* thing that changes
//! between the negative and positive halves of this journey is on-chain truth.
//!
//! The node runs with `cache.pull_through_require_authorized_origin`, so its
//! reactive cache-miss fill sits behind the chain-backed authorized-origin
//! directory (`ContentClaimed` → `OriginAssignment.getOrigins` → active
//! operator). Before the publisher claims `H` and the DAO ratifies the operator
//! set, the node is a plain cache: it serves what it already holds and refuses to
//! reach into its own backend for anything else. After ratification the same node
//! is a *recognized origin*: a client miss makes it fetch `H` opaquely from that
//! backend and serve bao-verified bytes — with the backend's location never
//! appearing on the wire.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::UpstreamRefused;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::{ChannelSession, ClientFixture};
use decdn_e2e::node::NodeFixture;
use decdn_protocol::client::{ClientMessage, StreamError};

const KIB: usize = 1024;
/// Every payload stays well under the 1 MiB voucher interval so the wire tap
/// captures a *complete* node→client delivery before the node pauses for the
/// closing voucher it will never receive.
const CACHED_LEN: usize = 96 * KIB;
const ORIGIN_LEN: usize = 192 * KIB;
const RANGE_LEN: usize = 160 * KIB;
const TAPPED_LEN: usize = 128 * KIB;
/// Deliberately not chunk-group aligned: the ranged origin fetch must return the
/// exact requested tail, not a conveniently rounded one.
const RANGE_OFFSET: u64 = 40_001;
const OVERALL_TIMEOUT: Duration = Duration::from_secs(900);

#[tokio::test(flavor = "multi_thread")]
async fn authorized_origin_fetches_opaquely_from_its_own_backend() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-NODE-08 exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // Distinct, byte-distinguishable payloads. All bytes are >= 0x80 so no
    // delivered chunk can accidentally spell an ASCII path and make the
    // wire-opacity scan below pass or fail by luck.
    let cached_payload = payload(0xC1, CACHED_LEN);
    let origin_payload = payload(0x88, ORIGIN_LEN);
    let range_payload = payload(0x9E, RANGE_LEN);
    let tapped_payload = payload(0xB7, TAPPED_LEN);

    // The node under test: an opaque `fs` backend plus the chain-backed
    // authorized-origin gate, and NO discovery peers — so it has no upstream to
    // pull from and a served backend-only blob can only have come from its own
    // `[cache.origin]`.
    let (node, cached) =
        NodeFixture::launch_authorized_origin(&chain, "US", &[&cached_payload]).await?;
    let cached_hash = *cached.first().context("launch returned no cached hash")?;

    // These three exist ONLY in the operator's backend — never warmed into its
    // store — so serving any of them requires origin recognition, not caching.
    let origin_hash = node.seed_origin_blob(&origin_payload)?;
    let range_hash = node.seed_origin_blob(&range_payload)?;
    let tapped_hash = node.seed_origin_blob(&tapped_payload)?;
    for (a, b) in [
        (cached_hash, origin_hash),
        (origin_hash, range_hash),
        (range_hash, tapped_hash),
        (cached_hash, tapped_hash),
    ] {
        anyhow::ensure!(a != b, "the fixture payloads must have distinct hashes");
    }

    let client = ClientFixture::new(&chain).await?;

    // ---------------------------------------------------------------- negative
    // Chain truth first: nothing claims H, and namespace 0's default-open
    // allow-list is empty, so the directory resolves NO authorized origin for it.
    // This is what the gate reads — assert it rather than assume it.
    assert!(chain.origins(U256::ZERO).await?.is_empty());
    assert!(
        chain
            .content_namespaces(b256(origin_hash))
            .await?
            .is_empty()
    );

    let mut unauthorized = client.open_session(&chain, &node).await?;

    // The cache role is untouched by the gate: a blob the node already holds is
    // served exactly as before. This is the control for the refusal below — it
    // proves the channel is live and the node is willing to deliver, so the
    // refusal cannot be a channel or liveness artifact.
    let from_cache = client
        .fetch_once(&mut unauthorized, cached_hash, 0)
        .await
        .context("an unauthorized node must still serve blobs it holds in cache")?;
    assert_eq!(from_cache, cached_payload);

    // The origin role is not: the gate refuses to INITIATE the backend fill for a
    // hash with no authorized origin, and the refusal is signed as the wire
    // `NotFound` every miss collapses onto.
    let refused = client
        .fetch_once(&mut unauthorized, origin_hash, 0)
        .await
        .expect_err("an unauthorized node must not serve from its backend");
    let code = refused
        .downcast_ref::<UpstreamRefused>()
        .map(|r| r.error.clone())
        .with_context(|| format!("expected a signed refusal, got: {refused:#}"))?;
    assert_eq!(
        code,
        StreamError::NotFound,
        "the authorized-origin gate must refuse before any backend read"
    );

    // ...and it really was a *pull-initiation* refusal: nothing was written to the
    // store, so the node did not quietly warm itself off the back of the request.
    assert!(
        !client.probe(&node, origin_hash).await?.body.has_blob,
        "a gated miss must not warm the cache"
    );
    // The ranged shape is gated identically — the gate sits above the range tier.
    client
        .fetch_once(&mut unauthorized, range_hash, RANGE_OFFSET)
        .await
        .expect_err("a ranged backend fetch is gated the same way");

    // -------------------------------------------------------------- ratification
    // Publisher claims the three backend-only hashes into its namespace and
    // proposes this operator as their origin; the DAO ratifies through the
    // assignment timelock.
    let publisher = PrivateKeySigner::random();
    let namespace = chain.create_namespace(&publisher).await?;
    for hash in [origin_hash, range_hash, tapped_hash] {
        chain
            .claim_content(&publisher, namespace, b256(hash))
            .await?;
    }
    chain
        .propose_assignment(&publisher, namespace, &[node.operator_addr()])
        .await?;
    chain.activate_assignment_after_timelock(namespace).await?;
    assert_eq!(
        chain.content_namespaces(b256(origin_hash)).await?,
        vec![namespace]
    );
    assert!(
        chain
            .is_authorized_origin(namespace, node.operator_addr())
            .await?,
        "the DAO-ratified assignment must name this operator as the namespace origin"
    );

    // ---------------------------------------------------------------- positive
    // Same node, same backend, same config — only the chain changed. `fetch`
    // opens a fresh channel (the timelock advanced chain time past the old one's
    // usefulness) and rides out the directory watcher's catch-up.
    let served = client.fetch(&chain, &node, origin_hash).await?;
    assert_eq!(
        served.bytes, origin_payload,
        "a recognized origin must serve the backend's bytes verbatim"
    );
    assert!(
        client.probe(&node, origin_hash).await?.body.has_blob,
        "the reactive backend fill must land the blob in the local store"
    );

    let mut authorized = client.open_session(&chain, &node).await?;

    // Ranged origin fetch: `range_hash` has never been in the store, so this span
    // is produced by the origin tier and bao-verified against the whole-blob hash
    // by the requester. Byte-exact against the backend's copy.
    let tail = client
        .fetch_once(&mut authorized, range_hash, RANGE_OFFSET)
        .await
        .context("ranged origin fetch")?;
    assert_eq!(
        tail.as_slice(),
        &range_payload[usize::try_from(RANGE_OFFSET)?..],
        "a ranged origin fetch must return the exact requested tail"
    );

    // ------------------------------------------------------------ wire opacity
    assert_backend_never_on_the_wire(&client, &node, &authorized, tapped_hash, &tapped_payload)
        .await?;

    Ok(())
}

/// Tap the raw `cdn/client/v1` frames of a delivery the node can only satisfy
/// from its opaque backend, and assert the backend's location is nowhere in them
/// (CLAUDE.md §Key Design Decisions: "No external origin URLs are ever exposed").
///
/// The scan is only worth anything if it can actually see the wire, so it starts
/// with a POSITIVE control: the blob hash *is* on the wire (echoed in the signed
/// `StreamResponse`), and if the search cannot find that, every "absent" assertion
/// below would be vacuously true.
async fn assert_backend_never_on_the_wire(
    client: &ClientFixture,
    node: &NodeFixture,
    session: &ChannelSession,
    hash: Hash,
    payload: &[u8],
) -> anyhow::Result<()> {
    assert!(
        !client.probe(node, hash).await?.body.has_blob,
        "the tapped blob must start absent, so the tap covers a real backend fill"
    );
    let frames = client.capture_delivery_wire(session, hash).await?;

    // The tap captured a real delivery, not a refusal: an `ok` response promising
    // the blob's true size, followed by every chunk of it.
    let (first, _) = decdn_protocol::decode_message::<ClientMessage>(&frames[0])
        .map_err(|e| anyhow::anyhow!("decode first wire frame: {e}"))?;
    let ClientMessage::StreamResponse(resp) = first else {
        anyhow::bail!("first frame was not a StreamResponse");
    };
    assert!(
        resp.body.ok,
        "tapped delivery was refused: {:?}",
        resp.error
    );
    assert_eq!(resp.body.total_bytes, payload.len() as u64);
    assert!(
        resp.body.redirect.is_none(),
        "an origin must serve its own backend, never redirect a client at it"
    );
    let delivered: usize = frames
        .iter()
        .filter_map(|f| decdn_protocol::decode_message::<ClientMessage>(f).ok())
        .filter_map(|(m, _)| match m {
            ClientMessage::ChunkData(c) => Some(c.bytes().len()),
            _ => None,
        })
        .sum();
    // `>=`, not `==`: chunks carry *wire* bytes — bao content plus interleaved
    // proof nodes (ADR 038 §Payment metering) — so a complete delivery is always
    // at least the content length. The byte-exactness of the assembled blob is
    // asserted by the ordinary paid fetches above; what matters here is that the
    // tap covered a whole delivery rather than a truncated one.
    assert!(
        delivered >= payload.len(),
        "the tap captured {delivered} of at least {} delivery bytes",
        payload.len()
    );

    // Positive control — the scan sees the wire.
    assert!(
        frames_contain(&frames, hash.as_bytes()),
        "wire scan is blind: the requested hash must appear in the captured frames"
    );

    // The needles: the backend root the daemon was configured with, every path
    // component of it, and the object key the `fs` adapter derived for this blob.
    let root = node.origin_root();
    let hex = hash.to_hex();
    let shard = hex.as_str().get(..2).context("blob hex too short")?;
    let mut needles: Vec<Vec<u8>> = vec![
        root.to_string_lossy().as_bytes().to_vec(),
        root.join(shard)
            .join(hex.as_str())
            .to_string_lossy()
            .as_bytes()
            .to_vec(),
        hex.as_bytes().to_vec(),
        b"file://".to_vec(),
    ];
    needles.extend(
        root.components()
            .filter_map(|c| c.as_os_str().to_str())
            // Single-character components (`/`) are noise, not a location leak.
            .filter(|c| c.len() > 1)
            .map(|c| c.as_bytes().to_vec()),
    );
    for needle in &needles {
        assert!(
            !frames_contain(&frames, needle),
            "opaque backend leaked onto the wire: {:?}",
            String::from_utf8_lossy(needle)
        );
    }
    Ok(())
}

/// Whether `needle` occurs verbatim in any captured frame.
fn frames_contain(frames: &[Vec<u8>], needle: &[u8]) -> bool {
    !needle.is_empty()
        && frames
            .iter()
            .any(|f| f.len() >= needle.len() && f.windows(needle.len()).any(|w| w == needle))
}

/// A deterministic payload whose every byte is >= 0x80, so no chunk on the wire
/// can accidentally contain an ASCII path fragment.
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let step = u8::try_from(i % 251).unwrap_or(0);
            0x80 | (seed ^ step).wrapping_mul(7) >> 1
        })
        .collect()
}

fn b256(hash: Hash) -> B256 {
    B256::from_slice(hash.as_bytes())
}
