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
//! reach into its own backend for anything else. After ratification the gate
//! opens: a client miss makes the same node fetch `H` opaquely from that backend
//! and serve bao-verified bytes — with the backend's location never appearing on
//! the wire.
//!
//! Scope of the gate, precisely: `pull_origin_gate_blocks` asks whether the
//! hash's **namespace** has any currently-authorized active origin. It does not
//! compare this node's operator address against the assignment, so this journey
//! asserts namespace authorization, not "this operator was individually
//! recognized". Whether that is the intended scope is tracked in #1368; the
//! journey is written to hold under either answer.

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

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::UpstreamRefused;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::{ChannelSession, ClientFixture};
use decdn_e2e::node::NodeFixture;
use decdn_protocol::client::{ClientMessage, StreamError};

const KIB: usize = 1024;
// Every payload stays well under the 1 MiB voucher interval. That matters for
// TAPPED_LEN in particular: the wire tap never pays, so it can only capture what
// the node emits before it blocks for a voucher — a sub-interval blob means that
// is the whole delivery.
const CACHED_LEN: usize = 96 * KIB;
const ORIGIN_LEN: usize = 192 * KIB;
const RANGE_LEN: usize = 160 * KIB;
const TAPPED_LEN: usize = 128 * KIB;
/// Not chunk-group aligned, so the served tail must be the exact requested one
/// rather than a conveniently rounded span.
///
/// Note this does *not* currently exercise `bao-range`'s alignment handling:
/// [`NodeFixture::seed_origin_blob`] writes no `{H}.obao4`, so
/// `FilesystemOrigin::fetch_range` reports `Unsupported`, the origin *range*
/// tier declines, and the request degrades to a whole-blob fill plus a
/// server-side `export_range`. Seeding the outboard to reach the range tier is
/// tracked separately.
const RANGE_OFFSET: u64 = 40_001;
const OVERALL_TIMEOUT: Duration = Duration::from_secs(900);
/// Shortest path component the wire-opacity scan treats as a location leak.
/// See the rationale at its use site in [`assert_backend_never_on_the_wire`].
const MIN_DISTINCTIVE_COMPONENT: usize = 6;

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

    // Distinct, byte-distinguishable payloads. All content bytes are >= 0x80, so
    // no delivered chunk can accidentally spell an ASCII path and fail the
    // wire-opacity scan by luck. (This covers content only — the interleaved bao
    // proof nodes are hash material and can be any byte.)
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
    // store — so serving any of them requires the origin gate to be open, and
    // cannot be satisfied by the node's cache role.
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
    // Chain truth first: the client fetches under namespace 0 (no namespace),
    // which has no authorized origins (ADR 002 § Namespace 0), so the directory
    // resolves NO authorized origin for the request. This is what the gate reads
    // — assert it rather than assume it.
    assert!(chain.origins(U256::ZERO).await?.is_empty());

    // The cache role is untouched by the gate: a blob the node already holds is
    // served exactly as before. That serve doubles as this session's warm-up, so
    // it is the control for every refusal below — it proves the node's serve path
    // holds the channel and is willing to deliver, which is exactly what a
    // refusal needs ruled out. `open_session` retries it, so the pre-observation
    // window cannot masquerade as a refusal later.
    let (mut unauthorized, from_cache) = client.open_session(&chain, &node, cached_hash).await?;
    assert_eq!(
        from_cache, cached_payload,
        "an unauthorized node must still serve blobs it holds in cache"
    );

    // The origin role is not: the gate refuses to INITIATE the backend fill for a
    // hash with no authorized origin, and the refusal is signed as the wire
    // `NotFound` every miss collapses onto.
    //
    // `NotFound` alone does not pin this to the gate — `ServeRejectReason::wire_error`
    // folds seven reject reasons onto it. What discriminates is the journey's
    // shape: same node, same backend, same config, and the identical fetch
    // succeeds below once the chain changes. The cached control above rules out a
    // dead channel; the store probe below rules out a fill that happened anyway.
    assert_refused_not_found(&client, &mut unauthorized, origin_hash, 0, "whole-blob").await?;

    // ...and it really was a *pull-initiation* refusal: nothing was written to the
    // store, so the node did not quietly warm itself off the back of the request.
    assert!(
        !client.probe(&node, origin_hash).await?.body.has_blob,
        "a gated miss must not warm the cache"
    );

    // The ranged shape is gated identically — the gate sits above the range tier.
    // Asserted to the same standard as the whole-blob case: a bare `expect_err`
    // here would be satisfied by a transport error or a stale nonce, and would
    // stay green if the gate stopped covering offset requests and the range tier
    // merely failed on its own.
    assert_refused_not_found(
        &client,
        &mut unauthorized,
        range_hash,
        RANGE_OFFSET,
        "ranged",
    )
    .await?;
    assert!(
        !client.probe(&node, range_hash).await?.body.has_blob,
        "a gated ranged miss must not warm the cache either"
    );

    // -------------------------------------------------------------- ratification
    // The publisher creates a namespace and proposes this operator as its origin;
    // the DAO ratifies through the assignment timelock. There is no per-hash claim
    // (ADR 002 § Hash-to-namespace association) — the namespace is the unit of
    // origin authority, and a request carries the namespace its content is
    // published under.
    let publisher = PrivateKeySigner::random();
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .propose_assignment(&publisher, namespace, &[node.operator_addr()])
        .await?;
    chain.activate_assignment_after_timelock(namespace).await?;
    assert_eq!(
        chain.origins(namespace).await?,
        vec![node.operator_addr()],
        "the DAO-ratified assignment must list this operator as the namespace's origin"
    );
    // Confirms the assignment activated and names this operator. Note the node's
    // gate does not itself read operator identity (#1368) — this asserts the
    // chain state the journey set up, not the predicate the gate evaluates.
    assert!(
        chain
            .is_authorized_origin(namespace, node.operator_addr())
            .await?,
        "the DAO-ratified assignment must have activated for this namespace"
    );

    // ---------------------------------------------------------------- positive
    // Same node, same backend, same config — only the chain changed. `fetch` is
    // used here rather than `fetch_once` because it RETRIES: the directory
    // watcher has to observe `AssignmentActivated` before the gate opens, and
    // there is no readiness signal for that. (`fetch` also opens its own fresh
    // channel as a side effect. That is incidental, not required — the 3-day
    // assignment timelock is far short of the 90-day channel duration, so the
    // `unauthorized` session is still perfectly usable here.)
    let served = client.fetch(&chain, &node, origin_hash).await?;
    assert_eq!(
        served.bytes, origin_payload,
        "a recognized origin must serve the backend's bytes verbatim"
    );
    assert!(
        client.probe(&node, origin_hash).await?.body.has_blob,
        "the reactive backend fill must land the blob in the local store"
    );

    // Warmed on the cached blob, so this fresh channel is registered in the
    // node's serve path before the single-shot fetch below depends on it.
    let (mut authorized, _) = client.open_session(&chain, &node, cached_hash).await?;

    // Ordering is load-bearing: the successful `fetch` above proves the watcher
    // has applied `AssignmentActivated`, and since all three `ContentClaimed`
    // were mined in earlier blocks and the sink applies logs in block order, it
    // transitively proves `range_hash`'s claim is in the directory too. Together
    // with the warm-up (which covers channel registration — a separate fact),
    // that is what lets the ranged fetch below use `fetch_once` (no retry)
    // safely. Do not reorder these two blocks.
    //
    // `range_hash` has never been in the store, so the span is produced by a
    // backend fill and bao-verified against the whole-blob hash by the requester.
    // Byte-exact against the backend's copy. (Which fill tier serves it — range
    // vs whole-blob — is discussed at `RANGE_OFFSET`.)
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
/// The scan is only worth anything if it can actually see the *delivered* wire,
/// so it carries a POSITIVE control: a slice of the payload's final third must be
/// found in the captured frames. If that is missing, every "absent" assertion
/// here would be vacuously true.
///
/// The control deliberately targets the tail rather than the blob hash. The hash
/// is echoed in the `StreamResponse` — frame 0 — so a hash-based control stays
/// green under *every* possible truncation and proves only that the byte search
/// runs. A late payload slice proves the capture actually reached the end.
///
/// Scope note: this is a regression tripwire, not proof of opacity. `cdn/client/v1`
/// has no string-typed field today (`redirect` is an `Option<NodeId>`), so a path
/// leak is currently unrepresentable rather than merely absent. The scan earns its
/// keep if someone later adds a diagnostic `String` to the protocol.
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
        "a served response must not be a redirect-to-peer (this is a `NodeId` \
         field, so it could never carry a backend location either way)"
    );

    // Count delivered content, and refuse to silently skip a frame that failed to
    // decode: dropping one can only push `delivered` down, but it would report as
    // a truncated capture and send the reader after the wrong subsystem.
    let mut undecodable = 0usize;
    let mut delivered = 0usize;
    for frame in &frames {
        match decdn_protocol::decode_message::<ClientMessage>(frame) {
            Ok((ClientMessage::ChunkData(c), _)) => delivered += c.bytes().len(),
            Ok(_) => {}
            Err(_) => undecodable += 1,
        }
    }
    anyhow::ensure!(
        undecodable == 0,
        "{undecodable} of {} captured frames failed to decode as a ClientMessage",
        frames.len()
    );
    // Chunks carry *wire* bytes — bao content plus interleaved proof nodes
    // (ADR 038 §Payment metering) — so a complete delivery strictly EXCEEDS the
    // content length. `>` rather than `>=` matters: TAPPED_LEN is an exact
    // multiple of the 1 KiB wire frame, so the full frames alone sum to
    // `payload.len()` and a `>=` test would tolerate losing the trailing partial
    // frame — the one place any trailing metadata would live.
    assert!(
        delivered > payload.len(),
        "the tap captured {delivered} bytes, not a complete delivery of {} content \
         bytes plus bao proof overhead",
        payload.len()
    );

    // The scan is searched over the concatenation, not frame by frame: frames are
    // 1 KiB (`CHUNK_SIZE`) and the composite path needle is ~110 bytes, so a real
    // leak would straddle a frame boundary about 10% of the time and a per-frame
    // scan would silently miss it. For a NEGATIVE assertion, concatenating can
    // only ever produce a false alarm, never a false pass.
    let wire = frames.concat();

    // Positive control — the scan sees the *end* of the wire, not just frame 0.
    // The payload's final bytes live in the last chunk group, so finding them
    // proves both that the byte search works and that the capture ran to
    // completion. A hash-based control could do neither: the hash is echoed in
    // frame 0, so it survives every possible truncation.
    let tail_probe = payload
        .get(payload.len().saturating_sub(32)..)
        .filter(|p| p.len() == 32)
        .context("payload too short for a tail control probe")?;
    assert!(
        contains(&wire, tail_probe),
        "wire scan is blind or the capture is truncated: the payload's final 32 \
         bytes must appear in the captured frames"
    );

    // The needles: the backend root the daemon was configured with (both as
    // configured and as canonicalized, since `FilesystemOrigin` canonicalizes at
    // construction), its distinctive path components, and the object key the `fs`
    // adapter derived for this blob.
    let root = node.origin_root();
    let canonical = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let hex = hash.to_hex();
    let shard = hex.as_str().get(..2).context("blob hex too short")?;
    let mut needles: Vec<Vec<u8>> = vec![
        path_bytes(root),
        path_bytes(&canonical),
        path_bytes(&root.join(shard).join(hex.as_str())),
        path_bytes(&canonical.join(shard).join(hex.as_str())),
        hex.as_bytes().to_vec(),
        b"file://".to_vec(),
    ];
    // Only DISTINCTIVE components. Short ones are shared with every path on the
    // system (`tmp`, `var`, the 2-char macOS `/var/folders/xy` shard) and carry no
    // signal — but the frames also carry several hundred bytes of uniformly random
    // bao parent hashes and signatures, so a 2- or 3-byte needle matches by chance
    // often enough to be a real flake source (~1% per run on macOS at length 2).
    // A genuine leak would surface the root, the object key, or the tempdir leaf,
    // all of which are far longer than this floor.
    needles.extend(
        root.components()
            .chain(canonical.components())
            .map(std::path::Component::as_os_str)
            .filter(|c| c.len() >= MIN_DISTINCTIVE_COMPONENT)
            .map(|c| c.as_encoded_bytes().to_vec()),
    );
    for needle in &needles {
        anyhow::ensure!(!needle.is_empty(), "empty needle would match vacuously");
        assert!(
            !contains(&wire, needle),
            "opaque backend leaked onto the wire: {:?}",
            String::from_utf8_lossy(needle)
        );
    }
    Ok(())
}

/// Raw bytes of `path`, without the lossy UTF-8 round-trip — a needle has to be
/// the bytes the daemon would actually emit, not a `U+FFFD`-substituted rendering
/// that could never match.
fn path_bytes(path: &std::path::Path) -> Vec<u8> {
    path.as_os_str().as_encoded_bytes().to_vec()
}

/// Whether `needle` occurs verbatim in `haystack`.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack.len() >= needle.len()
        && haystack.windows(needle.len()).any(|w| w == needle)
}

/// Assert a single-shot fetch of `hash` is refused with a *signed* wire
/// `NotFound`, rather than failing for some unrelated reason (transport error,
/// stale nonce, timeout). `label` names the shape under test in the failure.
async fn assert_refused_not_found(
    client: &ClientFixture,
    session: &mut ChannelSession,
    hash: Hash,
    byte_offset: u64,
    label: &str,
) -> anyhow::Result<()> {
    let refused = client
        .fetch_once(session, hash, byte_offset)
        .await
        .err()
        .with_context(|| {
            format!("{label}: an unauthorized node must not serve from its backend")
        })?;
    let code = refused
        .downcast_ref::<UpstreamRefused>()
        .map(|r| r.error.clone())
        .with_context(|| format!("{label}: expected a signed refusal, got: {refused:#}"))?;
    assert_eq!(code, StreamError::NotFound, "{label}: refusal code");
    Ok(())
}

/// A deterministic payload whose every byte is >= 0x80, so no *content* byte on
/// the wire can accidentally contain an ASCII path fragment. (Interleaved bao
/// proof nodes are hash material and are not covered by this — which is why the
/// needle floor in [`assert_backend_never_on_the_wire`] exists.)
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            // `i % 251` always fits a u8; state that rather than defaulting,
            // so raising the modulus later fails loudly instead of silently
            // flattening the distribution.
            let step = u8::try_from(i % 251).expect("i % 251 fits in u8");
            0x80 | (seed ^ step).wrapping_mul(7) >> 1
        })
        .collect()
}
