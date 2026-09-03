//! Live anvil-backed e2e for the per-signer abandonment bucket: a signer that
//! takes a pool's free ramp-floor credit and disconnects without paying debits
//! its OWN node-local, refilling leaky bucket (ADR 003 § Pool solvency,
//! per-signer abandonment allowance). The bucket is keyed by `(node, signer)`,
//! refills one credit window every `pool_floor_signer_refill_secs`, and holds
//! `pool_floor_signer_bucket_windows` windows. An abandoned floor never rolls
//! into any pool-wide total, so it throttles only the signer that spent it and
//! never locks out a co-tenant drawing on the same pool. The pool's own ceiling
//! (`remaining − M`) bounds only LIVE, in-flight reservations, which a
//! disconnect releases. `crates/node/tests/client_loopback.rs` proves this
//! against fakes (`sequential_same_signer_abandons_drain_the_bucket`,
//! `one_signer_at_its_share_does_not_lock_out_a_co_tenant`); this journey proves
//! it end to end against a real anvil chain, a real `decdn-node` daemon, and the
//! real paid `cdn/client/v1` wire.
//!
//! Shape: one owner opens and funds a `PaymentPool` with a deposit far above the
//! floor `M`, so pool solvency never bites and only the per-signer bucket can
//! refuse. The node runs with a deliberately small bucket
//! ([`BUCKET_WINDOWS`] windows) and a frozen refill ([`REFILL_SECS`]), so a
//! short burst of withheld floors drains one signer's allowance deterministically
//! before any refill returns a window.
//!
//! Two DISTINCT delegate signers — each holding its own owner-issued
//! [`decdn_incentive::Capability`] on the SAME `pool_id` — drive the journey:
//!
//! 1. **Burst throttle.** Signer A opens [`BUCKET_WINDOWS`] raw `cdn/client/v1`
//!    streams in sequence, each on a blob the node already has cached (a HIT). It
//!    lets the node stream the free floor, then disconnects WITHOUT ever paying a
//!    voucher ("withhold"). Each withhold debits one window into A's bucket. After
//!    the burst the bucket is at capacity, so A's NEXT admission is refused
//!    `NotFound` — the wire code every `ServeRejectReason` collapses onto. Because
//!    that blob is cached, availability can never explain the refusal; the
//!    node-local `decdn_serve_stream_rejected_signer_floor_at_cap_total` counter,
//!    read as a delta, pins the refusal to the per-signer floor throttle rather
//!    than a plain cache miss.
//!
//! 2. **Cross-signer isolation.** A DISTINCT signer B, drawing on the SAME pool at
//!    that same instant, is STILL admitted and streamed its floor — proving the
//!    bucket is signer-isolated: A's drained allowance never reduces B's, because
//!    abandoned floors are node-local and never join a pool-wide total. Signer B
//!    pulls a blob seeded only into the node's opaque origin backend (a genuine
//!    cache MISS, forcing a reactive origin fill), so the co-tenant admission also
//!    exercises the miss serve path.
//!
//! No admin surface exposes a signer's live bucket value (it is accounting, not
//! policy — see `crates/node/src/handlers/client/mod.rs`), so this journey
//! asserts what is observable end to end: which streams the node admits, which it
//! refuses, and the per-reason reject counter behind the collapsed `NotFound`.
//!
//! Driven at the `PoolContext` / raw-wire layer rather than through
//! [`decdn_e2e::client::ClientFixture`]: the fixture's `open_pool_session`
//! always opens a FRESH pool self-owned by its one signer, so it cannot express
//! "two distinct signers spending against one shared pool" — the exact shape the
//! isolation property needs. The lower-level pieces used here
//! (`decdn_client_pull::buyer_pool::open_pool`, `PoolContext`,
//! `decdn_incentive::Capability::sign`, and the raw `write_frame`/`read_frame`
//! wire helpers) are the same ones the fixture itself is built from.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` on
//! `PATH` and a built `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e abandonment_bucket_throttle
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::buyer_pool::open_pool;
use decdn_client_pull::{PoolContext, sign_client_binding};
use decdn_common::config::DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolState;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    Capability, SignedCapability, bind_node_id_domain, floor_micro, voucher_domain,
};
use decdn_protocol::client::{
    CHUNK_BYTES, ClientMessage, StreamError, StreamRequestExt, WireCapability,
};
use decdn_protocol::{ALPN_CLIENT, StreamRequest, encode_stream_request};
use iroh::{Endpoint, EndpointAddr};

/// The daemon's default `payment.rate_per_mb` (`crates/e2e/src/node.rs`'s
/// `render_config`). Left untouched — no `set_rate_per_mb` round trip is needed
/// since the floor cost below is derived from this exact value.
const RATE_PER_MB: u64 = 10;
/// A blob comfortably past one ramp-floor chunk
/// (`decdn_protocol::client::CHUNK_BYTES`): large enough that a withheld lane is
/// capped by the credit-window floor itself (delivered == reserved bytes, so the
/// abandon debits exactly one window into the signer's bucket on disconnect),
/// never by running out of content early. Only the lower bound is load-bearing;
/// the margin above it costs nothing but transfer time.
const BLOB_BYTES: usize = 4 * 1024 * 1024 + 65_536;
/// Owner-delegated finite spend cap on each delegate capability — far above one
/// floor's cost so it never itself binds; the per-signer bucket (not this cap)
/// is what this journey exercises.
const DELEGATE_CAP_MICRO_USDC: u64 = 10_000_000;
const DELEGATE_EXPIRY_SECS: u64 = 3_600;
/// Fixed request timestamp; the node does not gate this path on freshness and
/// this journey never validates a signed response, so a constant suffices.
const TIMESTAMP_US: u64 = 0x00c0_ffe1;
/// Budget for the very first frame of a stream: generous enough to absorb a
/// genuine cache-miss reactive backend fill (`try_local_populate`), not just
/// network RTT. Mirrors `ClientFixture::capture_delivery_wire`'s
/// `WIRE_TAP_FIRST_FRAME`.
const FIRST_FRAME_BUDGET: Duration = Duration::from_secs(30);
/// How long a withhold read waits between frames before deciding the node has
/// parked awaiting a voucher that will never come. Must stay below the node's
/// `VOUCHER_READ_TIMEOUT` (10s), matching `ClientFixture::capture_delivery_wire`'s
/// `WIRE_TAP_IDLE`.
const IDLE_BUDGET: Duration = Duration::from_secs(5);
/// How long the first request against the freshly-opened pool rides out the
/// node's pool-registration readiness window (its `getPool` view resolving the
/// pool) before treating a `NotFound` as a real refusal.
const READY_RETRY_BUDGET: Duration = Duration::from_secs(45);
/// Per-signer abandonment-bucket capacity for this journey, in credit windows.
/// Small so a short, fast burst drains it deterministically: [`BUCKET_WINDOWS`]
/// withholds fit, the next admission is throttled.
const BUCKET_WINDOWS: u64 = 2;
/// Per-signer LIVE concurrency cap, in windows. Ample: the burst is sequential
/// (one stream at a time, each released on disconnect), so the live cap never
/// binds — only the durable bucket does.
const LIVE_WINDOWS: u64 = 8;
/// Seconds to refill one bucket window. Frozen far above the burst's wall-clock
/// duration so no window is returned mid-burst; the throttle is reached purely by
/// the abandon count, not by racing the refill clock.
const REFILL_SECS: u64 = 3_600;
/// A brief settle after each withheld stream, giving the node time to observe the
/// disconnect and debit the signer's bucket before the next admission — the e2e
/// analog of the loopback suite's `await_pool_bucket`. The debit is synchronous in
/// the server's handling of the connection close; this only covers the loopback
/// close-propagation gap.
const WITHHOLD_SETTLE: Duration = Duration::from_millis(1_000);
/// The node-local per-reason reject counter behind the collapsed wire `NotFound`.
/// Both the per-signer live cap and the abandonment throttle bump it (they share
/// `ServeRejectReason::SignerFloorAtCap`); the burst here is sequential, so the
/// live cap cannot fire and a delta on this counter isolates the throttle.
const SIGNER_FLOOR_REJECT_METRIC: &str = "decdn_serve_stream_rejected_signer_floor_at_cap_total";
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn abandonment_bucket_throttles_a_bursting_signer_without_locking_out_a_co_tenant()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("abandonment-bucket throttle e2e exceeded the overall timeout")??;
    Ok(())
}

/// The pool deposit: far above the floor `M`, so pool solvency never refuses a
/// lane and the per-signer bucket is the ONLY thing that can. `M` =
/// [`DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC`] (1 USDC) — this journey
/// never overrides it. The headroom above `M` is a large multiple of one
/// ramp-floor at [`RATE_PER_MB`] (`floor_micro(RATE_PER_MB)`), which is tiny
/// (one MB of price), so this stays well within the buyer's minted balance while
/// leaving the pool solvent for far more floors than the burst ever draws.
fn deposit_micro_usdc() -> U256 {
    let m = U256::from(DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC);
    let floor = floor_micro(RATE_PER_MB);
    m + floor * U256::from(1_024u64)
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: signer A's burst, its throttled \
              admission, and signer B's co-tenant admission each depend on the \
              accumulator state the prior step left behind, so decomposing it would \
              thread that state through helpers without shortening the journey"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // The ramp floor a withheld lane is capped at, in the `usize` the
    // delivered-byte counters use. The blobs are deliberately much larger, so a
    // lane that parks short of this floor was capped by something other than the
    // credit window — a real bug, not the withhold point.
    let floor_bytes = usize::try_from(CHUNK_BYTES).unwrap_or(usize::MAX);

    // A HIT blob (warmed into the node's cache at launch) that signer A bursts
    // against, and a MISS blob (written only into the node's opaque origin
    // backend, so it reaches a client through a reactive origin fill — the
    // buffered `try_local_populate` route) that co-tenant signer B pulls. Both
    // exceed one ramp-floor interval so a withheld lane is capped by the credit
    // window itself, not by running out of content.
    let hit_blob = deterministic_blob(BLOB_BYTES, 0x5eed_0001);
    let (node, hit_hash) = NodeFixture::launch(&chain, "US", &hit_blob).await?;
    let miss_blob = deterministic_blob(BLOB_BYTES, 0x5eed_0002);
    let miss_hash = node
        .seed_origin_blob(&miss_blob)
        .context("seed origin-only miss blob")?;
    anyhow::ensure!(
        miss_hash != hit_hash,
        "hit and miss blobs must hash differently, or the miss lane would silently hit the cache"
    );

    // Shrink the per-signer abandonment bucket and freeze its refill, so a short
    // burst of withholds deterministically drains one signer's allowance. Applied
    // before any pool activity; the restart reopens the same warm cache.
    node.set_pool_floor_signer(BUCKET_WINDOWS, LIVE_WINDOWS, REFILL_SECS)
        .await
        .context("shrink per-signer abandonment bucket")?;

    // The pool OWNER: funded via `ClientFixture` for its ETH/USDC/allowance
    // plumbing and its loopback iroh endpoint, reused directly (not through
    // `ClientFixture::fetch`, which always opens its own fresh, generously funded
    // pool) so this journey controls the exact deposit and shares one pool across
    // two distinct signers.
    let owner = ClientFixture::new(&chain).await?;
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
    let bind_domain = bind_node_id_domain(chain.chain_id(), chain.addrs().capacity_bond);
    let own_node_id = alloy::primitives::B256::from(*owner.endpoint().id().as_bytes());

    let contract = PaymentPool::new(
        chain.addrs().payment_pool,
        chain.provider_for(owner.signer()),
    );
    let deposit = deposit_micro_usdc();
    let opened = open_pool(
        &contract,
        Arc::clone(owner.signer()),
        &voucher_dom,
        chain.usdc(),
        owner.address(),
        deposit,
    )
    .await
    .context("owner open pool")?;
    let pool_id = opened.state.pool_id;

    // Two DISTINCT delegate signers, neither funded with any ETH or USDC (mirrors
    // `cli_fetch_delegated.rs`: a delegate signs vouchers and its client binding
    // off-chain and issues no on-chain transaction of its own). Each holds its own
    // owner-issued, finitely-capped `Capability` naming it as `signer` on the SAME
    // `pool_id`, so the node keys a SEPARATE abandonment bucket for each.
    let delegate_expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before epoch")?
        .as_secs()
        + DELEGATE_EXPIRY_SECS;
    let (signer_a, cap_a) = delegate_lane(owner.signer(), pool_id, delegate_expiry, &voucher_dom)?;
    let signer_a_ctx = delegate_context(
        pool_id,
        owner.address(),
        chain.usdc(),
        deposit,
        node.operator_addr(),
        signer_a,
        cap_a,
        own_node_id,
        &bind_domain,
        &voucher_dom,
    )?;
    let (signer_b, cap_b) = delegate_lane(owner.signer(), pool_id, delegate_expiry, &voucher_dom)?;
    let signer_b_ctx = delegate_context(
        pool_id,
        owner.address(),
        chain.usdc(),
        deposit,
        node.operator_addr(),
        signer_b,
        cap_b,
        own_node_id,
        &bind_domain,
        &voucher_dom,
    )?;

    let target = dial_target(&node).await?;

    // Burst: signer A withholds `BUCKET_WINDOWS` times, each debiting one window
    // into its own bucket. The first withhold rides the pool-registration
    // readiness window; the rest go straight through, the pool now being known.
    for round in 0..BUCKET_WINDOWS {
        let outcome = if round == 0 {
            withhold_until_ready(owner.endpoint(), target.clone(), &signer_a_ctx, hit_hash).await?
        } else {
            open_and_withhold(owner.endpoint(), target.clone(), &signer_a_ctx, hit_hash).await?
        };
        let bytes = match outcome {
            WithholdOutcome::Delivered { bytes } => bytes,
            WithholdOutcome::Refused(reason) => anyhow::bail!(
                "burst withhold {round} (of {BUCKET_WINDOWS} that must fit the bucket) was \
                 refused ({reason:?}); the bucket should not throttle until the burst reaches \
                 capacity"
            ),
        };
        anyhow::ensure!(
            bytes >= floor_bytes,
            "burst withhold {round} delivered only {bytes} bytes before parking — short of the \
             {floor_bytes}-byte ramp floor, so something other than the credit window capped it"
        );
        // Let the node observe the disconnect and debit the bucket before the next
        // admission reads it.
        tokio::time::sleep(WITHHOLD_SETTLE).await;
    }

    // Throttle: signer A's bucket is now drained to capacity, so its next
    // admission is refused. The blob is the SAME cached HIT the burst used, so
    // availability can never be the reason — only the per-signer floor throttle.
    let rejected_before = node.scrape_metric(SIGNER_FLOOR_REJECT_METRIC).await?;
    let throttled =
        open_and_withhold(owner.endpoint(), target.clone(), &signer_a_ctx, hit_hash).await?;
    match throttled {
        WithholdOutcome::Delivered { bytes } => anyhow::bail!(
            "signer A was served {bytes} bytes after draining its abandonment bucket; the \
             per-signer throttle should have refused it once the {BUCKET_WINDOWS}-window bucket \
             was at capacity"
        ),
        WithholdOutcome::Refused(StreamError::NotFound) => {}
        WithholdOutcome::Refused(other) => anyhow::bail!(
            "signer A was refused, but with {other:?} rather than the expected `NotFound` \
             (`ServeRejectReason::wire_error` collapses the throttle refusal onto `NotFound`, \
             same as every other reject reason)"
        ),
    }
    let rejected_after = node.scrape_metric(SIGNER_FLOOR_REJECT_METRIC).await?;
    anyhow::ensure!(
        rejected_after == rejected_before + 1,
        "the throttle refusal must bump `{SIGNER_FLOOR_REJECT_METRIC}` by exactly one \
         (before={rejected_before}, after={rejected_after}); a plain cache miss would leave it \
         unchanged, and the blob is cached, so only the per-signer floor throttle can refuse it"
    );

    // Cross-signer isolation: a DISTINCT co-tenant signer B, drawing on the SAME
    // pool, is STILL admitted and streamed its floor — signer A's drained bucket
    // never reduced B's, because abandoned floors are node-local and signer-keyed,
    // not a pool-wide total. B pulls the MISS blob, so this admission also
    // exercises the reactive origin serve path.
    let cotenant = open_and_withhold(owner.endpoint(), target, &signer_b_ctx, miss_hash).await?;
    let cotenant_bytes = match cotenant {
        WithholdOutcome::Delivered { bytes } => bytes,
        WithholdOutcome::Refused(reason) => anyhow::bail!(
            "co-tenant signer B was refused ({reason:?}); a throttled signer must not lock out a \
             DISTINCT signer on the same pool — the abandonment bucket is per-signer, so B's is \
             fresh"
        ),
    };
    anyhow::ensure!(
        cotenant_bytes >= floor_bytes,
        "co-tenant signer B delivered only {cotenant_bytes} bytes before parking — short of the \
         {floor_bytes}-byte ramp floor, so it was not really served its free floor"
    );

    drop(node);
    Ok(())
}

/// Mint a distinct delegate signer plus its owner-issued, finitely-capped
/// [`Capability`] on `pool_id`. Deliberately unfunded (no ETH, no USDC): the
/// delegate never sends an on-chain transaction of its own.
fn delegate_lane(
    owner: &PrivateKeySigner,
    pool_id: alloy::primitives::B256,
    expiry: u64,
    voucher_dom: &alloy::dyn_abi::Eip712Domain,
) -> anyhow::Result<(PrivateKeySigner, SignedCapability)> {
    let delegate = PrivateKeySigner::random();
    let cap = Capability {
        signer: delegate.address(),
        spending_cap: U256::from(DELEGATE_CAP_MICRO_USDC),
        pool_id,
        expiry,
    }
    .sign(owner, voucher_dom)
    .context("owner sign delegate capability")?;
    Ok((delegate, cap))
}

/// Build a delegate's [`PoolContext`]: pinned to `pool_id`/`provider`, an
/// untouched (zero) lane watermark, this signer's own client identity binding,
/// and the owner-issued capability naming it. Mirrors `ClientFixture`'s private
/// `open_pool_session`, generalized to an arbitrary (not necessarily owner)
/// signer.
#[allow(clippy::too_many_arguments)]
fn delegate_context(
    pool_id: alloy::primitives::B256,
    owner_addr: Address,
    token: Address,
    deposit: U256,
    provider: Address,
    delegate: PrivateKeySigner,
    capability: SignedCapability,
    own_node_id: alloy::primitives::B256,
    bind_domain: &alloy::dyn_abi::Eip712Domain,
    voucher_dom: &alloy::dyn_abi::Eip712Domain,
) -> anyhow::Result<PoolContext> {
    let binding = sign_client_binding(&delegate, own_node_id, bind_domain)?;
    let state = BuyerPoolState::new(pool_id, owner_addr, token, deposit);
    Ok(
        PoolContext::for_pool(&state, Arc::new(delegate), voucher_dom.clone())
            .with_provider(provider, U256::ZERO, U256::ZERO)
            .with_client_binding(binding)
            .with_capability(capability),
    )
}

/// Outcome of a single withheld `cdn/client/v1` stream: either the node answered
/// `ok: true` and streamed some bytes before parking awaiting a voucher that never
/// comes, or it refused up front.
#[derive(Debug)]
enum WithholdOutcome {
    Delivered { bytes: usize },
    Refused(StreamError),
}

/// [`open_and_withhold`], retried while the failure is `NotFound` and `deadline`
/// has not elapsed — riding out the node's pool-registration readiness window
/// (its `getPool` view resolving a freshly-opened pool) the same way
/// `ClientFixture::fetch`/`open_session` do for the production paths.
///
/// Only the FIRST request against a fresh pool needs this: a not-yet-registered
/// pool refuses `NotFound` before ever reaching the floor gates, so no bucket is
/// touched by a readiness retry. Once a request succeeds the pool is known, and a
/// later `NotFound` is a real floor refusal — which is why the throttle assertion
/// uses the un-retried [`open_and_withhold`] directly.
async fn withhold_until_ready(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    hash: Hash,
) -> anyhow::Result<WithholdOutcome> {
    let deadline = tokio::time::Instant::now() + READY_RETRY_BUDGET;
    loop {
        match open_and_withhold(endpoint, target.clone(), ctx, hash).await? {
            WithholdOutcome::Refused(StreamError::NotFound)
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            outcome => return Ok(outcome),
        }
    }
}

/// Open one raw `cdn/client/v1` stream for `hash` under `ctx`, read whatever the
/// node sends, and NEVER pay a voucher — either the node refuses up front
/// (`StreamResponse { ok: false, .. }`), or it streams bytes up to the
/// credit-window floor and then parks; either way this closes the connection once
/// the node has said everything it is going to say, without ever advancing the
/// lane's voucher watermark. The disconnect is the withhold point: the node's
/// serve returns and the stream's floor reservation debits one window into the
/// signer's abandonment bucket. Mirrors `ClientFixture::capture_delivery_wire`,
/// generalized to report the open verdict rather than the raw frames.
async fn open_and_withhold(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    hash: Hash,
) -> anyhow::Result<WithholdOutcome> {
    let conn = endpoint
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: [0u8; 32],
        pool_id: ctx.pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: TIMESTAMP_US,
    };
    let ext = StreamRequestExt {
        binding: ctx.client_binding.clone(),
        capability: ctx.capability.as_ref().map(|signed| WireCapability {
            spending_cap: signed.capability.spending_cap.to_be_bytes(),
            expiry: signed.capability.expiry,
            owner_signature: signed.signature.as_bytes().to_vec(),
        }),
    };
    let payload = encode_stream_request(&req, Some(&ext)).context("encode StreamRequest")?;
    decdn_protocol::write_frame(&mut send, &payload)
        .await
        .context("write StreamRequest")?;

    let first = tokio::time::timeout(FIRST_FRAME_BUDGET, decdn_protocol::read_frame(&mut recv))
        .await
        .context("node sent no open frame within budget")?
        .context("read open frame")?;
    let (msg, tail) =
        decdn_protocol::decode_message::<ClientMessage>(&first).context("decode open frame")?;
    let ClientMessage::StreamResponse(resp) = msg else {
        anyhow::bail!("expected a StreamResponse open frame, got a different message");
    };
    // The refusal code rides in the trailing extension (ADR 013 §Tier 1), so the
    // open frame is read two-phase.
    let resp_ext =
        decdn_protocol::parse_stream_response_ext(tail).context("decode open frame extension")?;
    if !resp.body.ok {
        conn.close(0u32.into(), b"refused");
        return Ok(WithholdOutcome::Refused(
            resp_ext.error.unwrap_or(StreamError::InternalError),
        ));
    }

    // Drain further frames (ChunkData) without ever paying, until the node falls
    // idle awaiting the voucher we never send, or closes on its own.
    let mut delivered: usize = 0;
    loop {
        match tokio::time::timeout(IDLE_BUDGET, decdn_protocol::read_frame(&mut recv)).await {
            Ok(Ok(frame)) => delivered += frame.len(),
            // Idle: the node parked awaiting a voucher — the withhold point.
            Err(_) => break,
            Ok(Err(decdn_protocol::FrameError::Io(e)))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Ok(Err(e)) => {
                conn.close(0u32.into(), b"withhold read failed");
                return Err(anyhow::anyhow!(
                    "withhold read failed after {delivered} bytes: {e}"
                ));
            }
        }
    }
    conn.close(0u32.into(), b"withhold complete");
    Ok(WithholdOutcome::Delivered { bytes: delivered })
}

/// The loopback dial target for `node`, at the identity it is serving under right
/// now (`admin_v1_health`, not the launch-frozen `NodeFixture::node_id()` —
/// mirrors `ClientFixture`'s private `target` helper).
async fn dial_target(node: &NodeFixture) -> anyhow::Result<EndpointAddr> {
    Ok(
        EndpointAddr::new(node.current_node_id().await?).with_ip_addr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
        )),
    )
}

/// A deterministic pseudo-random blob of `len` bytes (xorshift32), so the two
/// blobs in this journey are large, non-trivially-compressible, and reproducible
/// without depending on a system RNG.
fn deterministic_blob(len: usize, seed: u32) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x = seed;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}
